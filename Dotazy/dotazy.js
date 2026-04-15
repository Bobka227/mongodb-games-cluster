// ============================================================
// DOTAZY - MongoDB Sharded Cluster (gamesdb)
// Kolekce: games_unified_validated
// Připojení: mongosh --host localhost --port 27017
//            -u admin -p admin --authenticationDatabase admin
// ============================================================
//
// STRUKTURA:
// Kategorie 1: Agregační a analytické dotazy           (dotazy 1–6)
// Kategorie 2: Propojování dat a vazby mezi datasety   (dotazy 7–12)
// Kategorie 3: Transformace a obohacení dat            (dotazy 13–18)
// Kategorie 4: Distribuce dat, cluster a replikace     (dotazy 19–24)
// Kategorie 5: Validace, indexy a fulltextové hledání  (dotazy 25–30)
// ============================================================

// ==============================================================
// KATEGORIE 1: AGREGAČNÍ A ANALYTICKÉ DOTAZY
// ==============================================================

// --- Dotaz 1: Multidimenzionální srovnání platforem ---
// Zadání: Porovnej všechny tři platformy (Steam, PlayStation, Nintendo)
// z hlediska počtu her, průměrné ceny, průměrného hodnocení kritiků,
// celkového počtu hodnocení a podílu her vydaných po roce 2018.
//
// Obecně: Pipeline kombinuje $group pro agregaci metrik, $project
// pro výpočet odvozených polí ($divide, $multiply, $round) a $sort
// pro seřazení výsledků. $cond s $gt umožňuje podmíněný součet
// (počítá jen hry splňující podmínku release_year > 2018).
// Konkrétně: Výsledek ukazuje, která platforma má nejmodernější katalog
// (highest modern_game_ratio) a nejlepší průměrné hodnocení.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $group: {
      _id: "$source_platform",
      total_games:    { $sum: 1 },
      avg_price:      { $avg: "$price" },
      avg_critic:     { $avg: "$critic_score" },
      total_ratings:  { $sum: { $add: [{ $ifNull: ["$positive_ratings", 0] },
                                       { $ifNull: ["$negative_ratings", 0] }] } },
      modern_games:   { $sum: { $cond: [{ $gt: ["$release_year", 2018] }, 1, 0] } }
    }
  },
  {
    $project: {
      platform:          "$_id",
      total_games:       1,
      avg_price:         { $round: ["$avg_price", 2] },
      avg_critic:        { $round: ["$avg_critic", 1] },
      total_ratings:     1,
      modern_game_ratio: {
        $round: [{ $multiply: [{ $divide: ["$modern_games", "$total_games"] }, 100] }, 1]
      },
      _id: 0
    }
  },
  { $sort: { avg_critic: -1 } }
]);


// --- Dotaz 2: Wilson score – skutečně nejlépe hodnocené hry na Steamu ---
// Zadání: Seřaď Steam hry podle statisticky spolehlivého hodnocení komunity
// (Wilson lower bound při 95% spolehlivosti), ne podle prostého průměru.
//
// Obecně: Wilson lower bound je statistická metrika zohledňující
// nejistotu při malém počtu hodnocení. Vzorec:
//   (p + z²/2n − z·√((p(1−p) + z²/4n)/n)) / (1 + z²/n)
// kde p = podíl pozitivních, z = 1.96, n = celkový počet hodnocení.
// Pipeline kombinuje $match, komplexní $project s $divide/$sqrt/$add
// a $sort. Výsledek je spolehlivější než pouhý avg nebo approval_ratio.
// Konkrétně: Hry s 100 hodnoceními a 90% pozitivními budou níže než
// hry s 50 000 hodnoceními a 88% pozitivními.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform:  "steam",
      positive_ratings: { $gt: 500 },
      negative_ratings: { $gt: 0 }
    }
  },
  {
    $project: {
      title: 1,
      positive_ratings: 1,
      negative_ratings: 1,
      n: { $add: ["$positive_ratings", "$negative_ratings"] },
      p: { $divide: ["$positive_ratings",
                     { $add: ["$positive_ratings", "$negative_ratings"] }] },
      z: { $literal: 1.96 }
    }
  },
  {
    $project: {
      title: 1,
      n: 1,
      p: 1,
      wilson_score: {
        $let: {
          vars: {
            z2n: { $divide: [{ $multiply: [1.96, 1.96] }, "$n"] }
          },
          in: {
            $divide: [
              { $subtract: [
                { $add: ["$p", { $divide: [{ $multiply: [1.96, 1.96] },
                                           { $multiply: [2, "$n"] }] }] },
                { $multiply: [1.96,
                  { $sqrt: { $divide: [
                    { $add: [{ $multiply: ["$p", { $subtract: [1, "$p"] }] },
                             { $divide: [{ $multiply: [1.96, 1.96] },
                                         { $multiply: [4, "$n"] }] }] },
                    "$n"
                  ]}}
                ]}
              ]},
              { $add: [1, { $divide: [{ $multiply: [1.96, 1.96] }, "$n"] }] }
            ]
          }
        }
      }
    }
  },
  { $sort:  { wilson_score: -1 } },
  { $limit: 15 },
  {
    $project: {
      title: 1,
      total_ratings: "$n",
      approval_pct: { $round: [{ $multiply: ["$p", 100] }, 1] },
      wilson_score:  { $round: ["$wilson_score", 4] },
      _id: 0
    }
  }
]);


// --- Dotaz 3: Vývoj průměrné ceny a hodnocení her v čase + top žánry podle epoch ---
// Zadání: Jak se vyvíjela průměrná cena her a hodnocení kritiků v čase?
// Rozděl hry do časových pásem a zjisti trend herního průmyslu,
// zároveň určete nejčastější žánry v jednotlivých obdobích.
//
// Obecně: $bucket rozděluje hry do časových intervalů podle release_year.
// $facet umožňuje v jednom průchodu dat získat více analytických pohledů.
// První větev počítá trend počtu her, průměrné ceny a hodnocení,
// druhá větev pomocí $unwind, $group a $sort určuje top žánry v jednotlivých epochách.
//
// Konkrétně: Výsledek ukazuje, zda hry v čase zdražují,
// jak se mění jejich hodnocení a které žánry v jednotlivých obdobích převažují.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      release_year: { $gte: 1995, $lte: 2024, $type: ["int", "long", "double"] }
    }
  },
  {
    $facet: {
      price_trend: [
        {
          $bucket: {
            groupBy: "$release_year",
            boundaries: [1995, 2000, 2005, 2010, 2015, 2018, 2021, 2025],
            default: "other",
            output: {
              game_count: { $sum: 1 },
              avg_price: { $avg: "$price" },
              avg_critic: { $avg: "$critic_score" },
              platforms: { $addToSet: "$source_platform" }
            }
          }
        },
        {
          $project: {
            epoch: "$_id",
            game_count: 1,
            avg_price: { $round: ["$avg_price", 2] },
            avg_critic: { $round: ["$avg_critic", 1] },
            platform_count: { $size: "$platforms" },
            _id: 0
          }
        }
      ],
      genre_by_epoch: [
        { $match: { genre: { $type: "array", $ne: [] } } },
        { $unwind: "$genre" },
        {
          $addFields: {
            epoch: {
              $switch: {
                branches: [
                  { case: { $and: [{ $gte: ["$release_year", 1995] }, { $lt: ["$release_year", 2005] }] }, then: "1995-2004" },
                  { case: { $and: [{ $gte: ["$release_year", 2005] }, { $lt: ["$release_year", 2015] }] }, then: "2005-2014" },
                  { case: { $and: [{ $gte: ["$release_year", 2015] }, { $lt: ["$release_year", 2025] }] }, then: "2015-2024" }
                ],
                default: "other"
              }
            }
          }
        },
        {
          $group: {
            _id: { epoch: "$epoch", genre: "$genre" },
            count: { $sum: 1 }
          }
        },
        { $sort: { "_id.epoch": 1, count: -1 } },
        {
          $group: {
            _id: "$_id.epoch",
            top_genres: {
              $push: {
                genre: "$_id.genre",
                count: "$count"
              }
            }
          }
        },
        {
          $project: {
            epoch: "$_id",
            top_genres: { $slice: ["$top_genres", 5] },
            _id: 0
          }
        }
      ]
    }
  }
]);


// --- Dotaz 4: Vydavatelé s hrami napříč více platformami ---
// Zadání: Kteří vydavatelé vydali hry alespoň na dvou platformách
// a jak rozsáhlé je jejich portfolio?
//
// Obecně: Pipeline používá $group s $addToSet pro získání množiny
// unikátních platforem, na kterých se hry daného vydavatele objevují.
// Pomocí $sum počítá celkový počet her, pomocí $avg průměrné hodnocení
// kritiků a pomocí $push ukládá ukázkové názvy titulů. $project následně
// dopočítá počet platforem přes $size a omezí počet ukázkových titulů
// pomocí $slice. $match filtruje pouze vydavatele přítomné alespoň
// na dvou platformách, poté následuje řazení a omezení výsledků.
//
// Konkrétně: Výsledek ukazuje, kteří vydavatelé jsou skutečně
// multi-platformní, na kolika platformách působí, kolik her celkem vydali
// a jaké tituly z jejich portfolia se v datech vyskytují.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null } } },
  {
    $group: {
      _id: "$publisher",
      platforms:    { $addToSet: "$source_platform" },
      total_games:  { $sum: 1 },
      avg_critic:   { $avg: "$critic_score" },
      sample_titles:{ $push: "$title" }
    }
  },
  {
    $project: {
      publisher:       "$_id",
      platforms:       1,
      platform_count:  { $size: "$platforms" },
      total_games:     1,
      avg_critic:      { $round: ["$avg_critic", 1] },
      sample_titles:   { $slice: ["$sample_titles", 3] },
      _id: 0
    }
  },
  { $match: { platform_count: { $gte: 2 } } },
  { $sort:  { platform_count: -1, total_games: -1 } },
  { $limit: 15 }
]);


// --- Dotaz 5: Korelace mezi cenou a průměrnou herní dobou podle žánru ---
// Zadání: Existuje vztah mezi cenou hry a průměrnou herní dobou?
// Analyzuj tento vztah zvlášť pro jednotlivé žánry a zjisti,
// ve kterých žánrech je korelace nejsilnější.
//
// Obecně: Pipeline nejprve vybere pouze dokumenty ze Steamu,
// které obsahují cenu, průměrnou herní dobu a neprázdné pole žánrů.
// Pomocí $unwind rozbalí jednotlivé žánry a následně v $group
// spočítá agregované hodnoty potřebné pro výpočet Pearsonova
// korelačního koeficientu. V části $project pak pomocí $let,
// $subtract, $multiply, $sqrt a $divide vypočítá korelaci mezi
// cenou a herní dobou pro každý žánr zvlášť.
//
// Konkrétně: Výsledek ukazuje, ve kterých žánrech mají dražší hry
// tendenci nabízet delší herní dobu a kde naopak tato závislost
// téměř neexistuje. To pomáhá interpretovat vztah mezi cenou
// a obsahem hry v různých segmentech trhu.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      genre: { $type: "array", $ne: [] },
      price: { $gt: 0, $type: ["double", "int", "long"] },
      average_playtime: { $gt: 0, $type: ["double", "int", "long"] }
    }
  },
  { $unwind: "$genre" },
  {
    $group: {
      _id: "$genre",
      n: { $sum: 1 },
      sum_x: { $sum: "$price" },
      sum_y: { $sum: "$average_playtime" },
      sum_xy: { $sum: { $multiply: ["$price", "$average_playtime"] } },
      sum_x2: { $sum: { $multiply: ["$price", "$price"] } },
      sum_y2: { $sum: { $multiply: ["$average_playtime", "$average_playtime"] } }
    }
  },
  { $match: { n: { $gte: 50 } } },
  {
    $project: {
      genre: "$_id",
      n: 1,
      pearson_r: {
        $let: {
          vars: {
            num: {
              $subtract: [
                { $multiply: ["$n", "$sum_xy"] },
                { $multiply: ["$sum_x", "$sum_y"] }
              ]
            },
            den: {
              $sqrt: {
                $multiply: [
                  {
                    $subtract: [
                      { $multiply: ["$n", "$sum_x2"] },
                      { $multiply: ["$sum_x", "$sum_x"] }
                    ]
                  },
                  {
                    $subtract: [
                      { $multiply: ["$n", "$sum_y2"] },
                      { $multiply: ["$sum_y", "$sum_y"] }
                    ]
                  }
                ]
              }
            }
          },
          in: {
            $cond: [
              { $eq: ["$$den", 0] },
              null,
              { $divide: ["$$num", "$$den"] }
            ]
          }
        }
      },
      avg_price: { $round: [{ $divide: ["$sum_x", "$n"] }, 2] },
      avg_playtime: { $round: [{ $divide: ["$sum_y", "$n"] }, 1] },
      _id: 0
    }
  },
  { $sort: { pearson_r: -1 } },
  { $limit: 10 }
]);


// --- Dotaz 6: Klouzavý průměr hodnocení kritiků – $setWindowFields ---
// Zadání: Jak se vyvíjelo hodnocení kritiků Nintendo her rok po roce?
// Zobraz roční průměr a 3letý klouzavý průměr pro odfiltrování šumu.
//
// Obecně: $group nejprve agreguje data na úroveň jednotlivých roků
// a vypočítá roční průměr hodnocení kritiků a počet her. Následné
// $setWindowFields aplikuje okenní funkce nad seřazenými roky.
// Okno range: [-1, 1] zahrnuje předchozí, aktuální a následující rok,
// takže vzniká 3letý klouzavý průměr. Současně se počítá i kumulativní
// počet her od začátku časové řady.
//
// Konkrétně: Výsledek ukazuje trend průměrného hodnocení Nintendo her
// v čase a jeho vyhlazenou podobu. Zároveň je vidět, kolik her vstupuje
// do výpočtu v jednotlivých letech, což je důležité pro interpretaci,
// protože v některých rocích je počet her velmi nízký.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "nintendo",
      release_year:    { $gte: 2000, $lte: 2023,
                         $type: ["int","long","double"] },
      critic_score:    { $gt: 0, $type: ["int","long","double"] }
    }
  },
  {
    $group: {
      _id:        "$release_year",
      avg_critic: { $avg: "$critic_score" },
      game_count: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } },
  {
    $setWindowFields: {
      sortBy: { _id: 1 },
      output: {
        moving_avg_3y: {
          $avg: "$avg_critic",
          window: { range: [-1, 1] }
        },
        cumulative_games: {
          $sum: "$game_count",
          window: { documents: ["unbounded", "current"] }
        }
      }
    }
  },
  {
    $project: {
      year:             "$_id",
      yearly_avg:       { $round: ["$avg_critic",    1] },
      moving_avg_3y:    { $round: ["$moving_avg_3y", 1] },
      game_count:       1,
      cumulative_games: 1,
      _id: 0
    }
  }
]);


// ==============================================================
// KATEGORIE 2: PROPOJOVÁNÍ DAT A VAZBY MEZI DATASETY
// ==============================================================

// --- Dotaz 7: Příprava kolekce publishers_metadata + $lookup ---
// Zadání: Vytvoř referenční kolekci vydavatelů se statistikami
// a spoj ji s herními záznamy pro obohacení dat.
//
// Obecně: Dotaz je rozdělen do tří kroků.
// V prvním kroku se odstraní stará pomocná kolekce publishers_metadata,
// aby nevznikaly duplicity ze starších běhů. Ve druhém kroku se nad
// touto kolekcí vytvoří unique index nad polem publisher_name, protože
// následný $merge používá toto pole jako jednoznačný klíč pro párování.
// Ve třetím kroku se z hlavní kolekce vytvoří agregované statistiky
// vydavatelů a uloží se do publishers_metadata. Následně se pomocí
// $lookup propojí konkrétní hry s těmito metadaty vydavatele.
//
// Konkrétně: Výsledek ukazuje dražší Steam hry s vyšším počtem
// pozitivních hodnocení a zároveň je obohacuje o informace o vydavateli,
// například kolik titulů celkem vydal, na kolika platformách působí
// a jaká je průměrná cena jeho her.


// Krok 0: odstranění staré pomocné kolekce
db.getSiblingDB("gamesdb").publishers_metadata.drop();


// Krok 1: vytvoření unique indexu pro bezpečný $merge podle publisher_name
db.getSiblingDB("gamesdb").publishers_metadata.createIndex(
  { publisher_name: 1 },
  { unique: true }
);


// Krok 2: vytvoření pomocné kolekce publishers_metadata
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null } } },
  {
    $group: {
      _id: "$publisher",
      total_titles: { $sum: 1 },
      platforms: { $addToSet: "$source_platform" },
      avg_price: { $avg: "$price" },
      avg_critic: { $avg: "$critic_score" }
    }
  },
  {
    $project: {
      publisher_name: "$_id",
      total_titles: 1,
      platform_count: { $size: "$platforms" },
      platforms: 1,
      avg_price: { $round: ["$avg_price", 2] },
      avg_critic: { $round: ["$avg_critic", 1] },
      _id: 0
    }
  },
  {
    $merge: {
      into: "publishers_metadata",
      on: "publisher_name",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
]);


// Krok 3: propojení her s metadaty vydavatele pomocí $lookup
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      price: { $gt: 20, $type: ["int", "long", "double"] },
      positive_ratings: { $gt: 1000, $type: ["int", "long", "double"] }
    }
  },
  {
    $lookup: {
      from: "publishers_metadata",
      localField: "publisher",
      foreignField: "publisher_name",
      as: "pub_stats"
    }
  },
  { $unwind: { path: "$pub_stats", preserveNullAndEmptyArrays: true } },
  {
    $project: {
      title: 1,
      publisher: 1,
      price: 1,
      positive_ratings: 1,
      "pub_stats.total_titles": 1,
      "pub_stats.platform_count": 1,
      "pub_stats.avg_price": 1,
      price_vs_pub_avg: {
        $round: [
          {
            $subtract: [
              "$price",
              { $ifNull: ["$pub_stats.avg_price", 0] }
            ]
          },
          2
        ]
      },
      _id: 0
    }
  },
  { $sort: { price_vs_pub_avg: -1 } },
  { $limit: 15 }
]);


// --- Dotaz 8: Hry přítomné na více platformách ($lookup self-join) ---
// Zadání: Najdi hry, které existují jak na Steamu, tak na jiné platformě.
// Pro každou takovou hru porovnej hodnocení napříč platformami.
//
// Obecně: $lookup s pipeline umožňuje self-join – spojení kolekce
// samu se sebou. Normalizace názvu přes $toLower + $trim odstraní
// rozdíly ve velikosti písmen. $match uvnitř lookup pipeline filtruje
// jen jiné platformy. $project pak sestaví srovnávací dokument.
// Konkrétně: Hry jako FIFA nebo GTA mohou mít různá hodnocení na
// různých platformách – dotaz to kvantifikuje.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      title: { $ne: null }
    }
  },
  {
    $addFields: {
      title_norm: { $toLower: { $trim: { input: "$title" } } }
    }
  },
  {
    $group: {
      _id: "$title_norm",
      titles: { $addToSet: "$title" },
      platforms: { $addToSet: "$source_platform" },
      entries: {
        $push: {
          source_platform: "$source_platform",
          critic_score: "$critic_score",
          user_score: "$user_score",
          price: "$price",
          title: "$title"
        }
      }
    }
  },
  {
    $match: {
      platforms: { $all: ["steam"] },
      $expr: { $gt: [{ $size: "$platforms" }, 1] }
    }
  },
  {
    $project: {
      title: { $arrayElemAt: ["$titles", 0] },
      platform_count: { $size: "$platforms" },
      entries: {
        $filter: {
          input: "$entries",
          as: "e",
          cond: {
            $or: [
              { $ne: ["$$e.price", null] },
              { $ne: ["$$e.critic_score", null] },
              { $ne: ["$$e.user_score", null] }
            ]
          }
        }
      },
      _id: 0
    }
  },
  {
    $match: {
      "entries.1": { $exists: true }
    }
  },
  { $sort: { platform_count: -1 } },
  { $limit: 20 }
]);


// --- Dotaz 9: $lookup na config.chunks – distribuce dat na shardech ---
// Zadání: Pro každý shard zjisti, kolik chunků kolekce
// games_unified_validated drží, jaký je jejich rozsah shard klíče
// a na kterém hostiteli (host) shard běží.
//
// Obecně: config.chunks je systémová kolekce MongoDB uchovávající
// metadata o rozdělení dat mezi shardy. UUID kolekce se nejprve
// načte přes findOne() na config.collections a použije jako filtr.
// $group počítá chunky na shard, $lookup obohacuje o hostname shardu
// z config.shards. $project s $slice zobrazí ukázkový rozsah klíčů.
// Konkrétně: Ukazuje reálné rozložení dat – kolik chunků (a tedy
// přibližně jaký % dat) leží na každém ze tří shardů.
const colMeta = db.getSiblingDB("config").collections
  .findOne({ _id: "gamesdb.games_unified_validated" });
const colUUID = colMeta ? colMeta.uuid : null;

if (!colUUID) {
  print("Kolekce nebyla nalezena v config.collections – není shardována.");
} else {
  db.getSiblingDB("config").chunks.aggregate([
    { $match: { uuid: colUUID } },
    {
      $group: {
        _id:         "$shard",
        chunk_count: { $sum: 1 },
        min_keys:    { $push: "$min" },
        max_keys:    { $push: "$max" }
      }
    },
    {
      $lookup: {
        from:         "shards",
        localField:   "_id",
        foreignField: "_id",
        as:           "shard_info"
      }
    },
    { $unwind: "$shard_info" },
    {
      $project: {
        shard:           "$_id",
        chunk_count:     1,
        host:            "$shard_info.host",
        sample_min_keys: { $slice: ["$min_keys", 2] },
        _id: 0
      }
    },
    { $sort: { shard: 1 } }
  ]);
}


// --- Dotaz 10: $unionWith – srovnání levných vs drahých her ---
// Zadání: Porovnej levné a drahé Steam hry podle ceny,
// počtu titulů a průměrného počtu pozitivních hodnocení.
//
// Obecně: $unionWith spojí dva různé podvýběry stejné kolekce
// do jednoho proudu dokumentů. Každému podvýběru je přiřazen
// štítek price_group a následně se pomocí $group vypočítají
// souhrnné statistiky pro levné a drahé hry.
//
// Konkrétně: Výsledek ukazuje, zda dražší Steam hry mají
// více pozitivních hodnocení než levné hry.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      price: { $gt: 0, $lte: 10, $type: ["int", "long", "double"] },
      positive_ratings: { $gt: 0, $type: ["int", "long", "double"] }
    }
  },
  {
    $addFields: { price_group: "cheap" }
  },
  {
    $unionWith: {
      coll: "games_unified_validated",
      pipeline: [
        {
          $match: {
            source_platform: "steam",
            price: { $gt: 30, $type: ["int", "long", "double"] },
            positive_ratings: { $gt: 0, $type: ["int", "long", "double"] }
          }
        },
        {
          $addFields: { price_group: "expensive" }
        }
      ]
    }
  },
  {
    $group: {
      _id: "$price_group",
      total_games: { $sum: 1 },
      avg_price: { $avg: "$price" },
      avg_positive_ratings: { $avg: "$positive_ratings" }
    }
  },
  {
    $project: {
      price_group: "$_id",
      total_games: 1,
      avg_price: { $round: ["$avg_price", 2] },
      avg_positive_ratings: { $round: ["$avg_positive_ratings", 0] },
      _id: 0
    }
  }
]);


// --- Dotaz 11: $lookup s pipeline – top hra vydavatele a průměr portfolia ---
// Zadání: Pro top vydavatele podle počtu her zobraz jejich nejlépe
// hodnocenou hru a porovnej ji s průměrným hodnocením vydavatele.
//
// Obecně: Nejprve se pomocí $group spočítá počet her a průměrné
// hodnocení kritiků pro každého vydavatele. Poté se pomocí $lookup
// s vnořenou pipeline dohledá nejlépe hodnocená hra daného vydavatele.
// Výsledný $project vypočítá rozdíl mezi nejlepším titulem a průměrem
// jeho portfolia.
//
// Konkrétně: Dotaz ukazuje, zda má vydavatel vyrovnané portfolio,
// nebo zda jeho nejlepší hra výrazně převyšuje běžnou úroveň ostatních titulů.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null } } },
  {
    $group: {
      _id: "$publisher",
      game_count: { $sum: 1 },
      avg_critic: { $avg: "$critic_score" },
      rated_games: {
        $sum: {
          $cond: [
            { $in: [{ $type: "$critic_score" }, ["int", "long", "double"]] },
            1,
            0
          ]
        }
      }
    }
  },
  { $match: { rated_games: { $gt: 0 } } },
  { $sort: { game_count: -1 } },
  { $limit: 10 },
  {
    $lookup: {
      from: "games_unified_validated",
      let: { pub: "$_id" },
      pipeline: [
        {
          $match: {
            $expr: { $eq: ["$publisher", "$$pub"] },
            critic_score: { $type: ["int", "long", "double"] }
          }
        },
        { $sort: { critic_score: -1 } },
        { $limit: 1 },
        {
          $project: {
            title: 1,
            critic_score: 1,
            source_platform: 1,
            _id: 0
          }
        }
      ],
      as: "best_game"
    }
  },
  { $unwind: "$best_game" },
  {
    $project: {
      publisher: "$_id",
      game_count: 1,
      rated_games: 1,
      avg_critic: { $round: ["$avg_critic", 1] },
      best_title: "$best_game.title",
      best_score: "$best_game.critic_score",
      best_platform: "$best_game.source_platform",
      score_above_avg: {
        $round: [
          { $subtract: ["$best_game.critic_score", "$avg_critic"] },
          1
        ]
      },
      _id: 0
    }
  },
  { $sort: { score_above_avg: -1 } }
]);


// --- Dotaz 12: Flagship tituly vydavatelů napříč platformami ---
// Zadání: Najdi nejlépe hodnocené hry každého vydavatele a označ
// je jako "flagship", pokud vydavatel působí alespoň na dvou platformách.
//
// Obecně: V první části se pro každého vydavatele spočítá počet titulů
// a počet platforem ze všech jeho her. Současně se určí maximální
// hodnocení kritiků pouze z těch her, které critic_score skutečně mají.
// Výsledek se uloží do pomocné kolekce publisher_flagship_stats.
// Ve druhé části se tato pomocná kolekce propojí s hlavní kolekcí her
// a ponechají se pouze hry, které dosahují maximálního hodnocení
// svého vydavatele.
//
// Konkrétně: Výsledkem je seznam flagship titulů, tedy her, které jsou
// vrcholem portfolia vydavatelů působících na více platformách.


// Krok 0: smaž starou pomocnou kolekci
db.getSiblingDB("gamesdb").publisher_flagship_stats.drop();


// Krok 1: vytvoř statistiky vydavatelů správně
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      publisher: { $ne: null }
    }
  },
  {
    $group: {
      _id: "$publisher",
      total_titles: { $sum: 1 },
      platforms: { $addToSet: "$source_platform" },
      max_critic: {
        $max: {
          $cond: [
            { $in: [{ $type: "$critic_score" }, ["int", "long", "double"]] },
            "$critic_score",
            null
          ]
        }
      }
    }
  },
  {
    $project: {
      publisher_name: "$_id",
      total_titles: 1,
      platform_count: { $size: "$platforms" },
      max_critic: 1,
      _id: 0
    }
  },
  {
    $match: {
      max_critic: { $type: ["int", "long", "double"] }
    }
  },
  {
    $merge: {
      into: "publisher_flagship_stats",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
]);


// Krok 2: najdi skutečné flagship hry
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      publisher: { $ne: null },
      critic_score: { $type: ["int", "long", "double"] }
    }
  },
  {
    $lookup: {
      from: "publisher_flagship_stats",
      localField: "publisher",
      foreignField: "publisher_name",
      as: "pub_meta"
    }
  },
  { $unwind: "$pub_meta" },
  {
    $match: {
      "pub_meta.platform_count": { $gte: 2 },
      $expr: { $eq: ["$critic_score", "$pub_meta.max_critic"] }
    }
  },
  {
    $project: {
      title: 1,
      source_platform: 1,
      critic_score: 1,
      publisher: 1,
      pub_total_titles: "$pub_meta.total_titles",
      pub_platform_count: "$pub_meta.platform_count",
      pub_max_critic: "$pub_meta.max_critic",
      is_flagship: "flagship",
      _id: 0
    }
  },
  { $sort: { critic_score: -1, pub_total_titles: -1 } },
  { $limit: 20 }
]);


// ==============================================================
// KATEGORIE 3: TRANSFORMACE A OBOHACENÍ DAT
// ==============================================================

// --- Dotaz 13: Klasifikace her do quality tiers a price tiers pomocí $switch ---
// Zadání: Přiřaď každé hře kategorii kvality podle hodnocení kritiků
// a samostatný cenový tier podle ceny. Následně zobraz souhrnnou
// statistiku těchto kombinací.
//
// Obecně: $addFields přidává odvozená pole quality_tier a price_tier.
// $switch slouží k vícevětvovému podmíněnému rozhodování. Hodnocení
// a cena jsou klasifikovány odděleně, takže dotaz není závislý
// na současné přítomnosti obou polí v každém dokumentu. Následný
// $group agreguje hry podle kombinace obou tierů a vypočítá jejich
// počet, průměrnou cenu, průměrné hodnocení a zastoupené platformy.
//
// Konkrétně: Výsledek ukazuje, jak jsou hry rozloženy mezi kvalitativní
// a cenové kategorie a na kterých platformách se tyto kombinace
// vyskytují nejčastěji.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $addFields: {
      quality_tier: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $in: [{ $type: "$critic_score" }, ["int", "long", "double"]] },
                  { $gte: ["$critic_score", 8.5] }
                ]
              },
              then: "Excellent"
            },
            {
              case: {
                $and: [
                  { $in: [{ $type: "$critic_score" }, ["int", "long", "double"]] },
                  { $gte: ["$critic_score", 7.5] }
                ]
              },
              then: "Strong"
            },
            {
              case: {
                $and: [
                  { $in: [{ $type: "$critic_score" }, ["int", "long", "double"]] },
                  { $gte: ["$critic_score", 6.0] }
                ]
              },
              then: "Average"
            }
          ],
          default: "Unknown"
        }
      },
      price_tier: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $in: [{ $type: "$price" }, ["int", "long", "double"]] },
                  { $eq: ["$price", 0] }
                ]
              },
              then: "Free"
            },
            {
              case: {
                $and: [
                  { $in: [{ $type: "$price" }, ["int", "long", "double"]] },
                  { $lte: ["$price", 9.99] }
                ]
              },
              then: "Budget"
            },
            {
              case: {
                $and: [
                  { $in: [{ $type: "$price" }, ["int", "long", "double"]] },
                  { $lte: ["$price", 29.99] }
                ]
              },
              then: "Standard"
            },
            {
              case: {
                $and: [
                  { $in: [{ $type: "$price" }, ["int", "long", "double"]] },
                  { $lte: ["$price", 59.99] }
                ]
              },
              then: "Premium"
            }
          ],
          default: "Luxury"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        quality: "$quality_tier",
        price: "$price_tier"
      },
      game_count: { $sum: 1 },
      avg_critic: { $avg: "$critic_score" },
      avg_price: { $avg: "$price" },
      platforms: { $addToSet: "$source_platform" }
    }
  },
  { $sort: { "_id.quality": 1, "_id.price": 1 } },
  {
    $project: {
      quality_tier: "$_id.quality",
      price_tier: "$_id.price",
      game_count: 1,
      avg_critic: { $round: ["$avg_critic", 1] },
      avg_price: { $round: ["$avg_price", 2] },
      platforms: 1,
      _id: 0
    }
  }
]);


// --- Dotaz 14: $replaceRoot + $mergeObjects – zploštění vnořeného raw_source ---
// Zadání: Vytvoř pohled na Steam hry kde jsou pole z raw_source
// (platforms, required_age, achievements) povýšena na úroveň dokumentu.
//
// Obecně: $replaceRoot nahradí kořen dokumentu novým objektem.
// $mergeObjects sloučí více objektů do jednoho – zde spojuje
// původní dokument s rozbaleným raw_source. Výsledek je dokument
// bez vnořování, vhodný pro export nebo reporting.
// Konkrétně: Místo game.raw_source.achievements bude přímo game.achievements.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      raw_source: { $type: "object" }
    }
  },
  {
    $replaceRoot: {
      newRoot: {
        $mergeObjects: [
          {
            title:            "$title",
            publisher:        "$publisher",
            release_year:     "$release_year",
            price:            "$price",
            positive_ratings: "$positive_ratings",
            negative_ratings: "$negative_ratings",
            average_playtime: "$average_playtime",
            genre:            "$genre"
          },
          "$raw_source"
        ]
      }
    }
  },
  {
    $project: {
      title: 1,
      publisher: 1,
      release_year: 1,
      price: 1,
      achievements: 1,
      required_age: 1,
      platforms: 1,
      median_playtime: 1,
      positive_ratings: 1
    }
  },
  { $match: { achievements: { $gt: 50 } } },
  { $sort:  { achievements: -1 } },
  { $limit: 15 }
]);


// --- Dotaz 15: $map + $filter – transformace a filtrování pole features ---
// Zadání: Pro každou hru na Steamu extrahuj jen "multiplayer" features
// (obsahující slova "Multi", "Co-op", "Online"), převeď je na lowercase
// a spočítej hry podle počtu multiplayer features.
//
// Obecně: $map aplikuje výraz na každý prvek pole a vrátí nové pole.
// $filter odstraní prvky nesplňující podmínku. $regexMatch testuje
// shodu s regulárním výrazem. $size počítá délku výsledného pole.
// Konkrétně: Ukazuje kolik Steam her podporuje 1, 2, 3+ multiplayer
// features a jaká je jejich průměrná délka hraní.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      features: { $type: "array", $ne: [] }
    }
  },
  {
    $project: {
      title: 1,
      average_playtime: 1,
      multiplayer_features: {
        $map: {
          input: {
            $filter: {
              input: "$features",
              cond: {
                $or: [
                  { $regexMatch: { input: "$$this", regex: "Multi", options: "i" } },
                  { $regexMatch: { input: "$$this", regex: "Co-op", options: "i" } },
                  { $regexMatch: { input: "$$this", regex: "Online", options: "i" } }
                ]
              }
            }
          },
          as: "f",
          in: { $toLower: "$$f" }
        }
      }
    }
  },
  {
    $addFields: {
      mp_feature_count: { $size: "$multiplayer_features" }
    }
  },
  { $match: { mp_feature_count: { $gte: 1 } } },
  {
    $group: {
      _id:          "$mp_feature_count",
      game_count:   { $sum: 1 },
      avg_playtime: { $avg: "$average_playtime" },
      sample_games: { $push: "$title" }
    }
  },
  {
    $project: {
      mp_feature_count: "$_id",
      game_count:       1,
      avg_playtime_hrs: { $round: [{ $divide: [{ $ifNull: ["$avg_playtime", 0] }, 60] }, 1] },
      sample_games:     { $slice: ["$sample_games", 3] },
      _id: 0
    }
  },
  { $sort: { mp_feature_count: 1 } }
]);


// --- Dotaz 16: $reduce – sloučení features do řetězce a feature richness score ---
// Zadání: Pro každou Steam hru spoj seznam features do jednoho řetězce
// a vypočítej odvozené skóre jako počet features × počet pozitivních hodnocení.
// Následně zobraz hry s nejvyšší hodnotou tohoto ukazatele.
//
// Obecně: $reduce iteruje přes pole features a postupně z něj skládá
// textový řetězec. $size určuje počet features a $multiply následně
// kombinuje počet features s positive_ratings do odvozeného ukazatele
// feature_richness_score. Výsledkem je transformovaný pohled na hry,
// které mají bohatou sadu funkcí a zároveň silnou uživatelskou odezvu.
//
// Konkrétně: Dotaz vyhledává Steam hry, které kombinují větší počet
// funkcí s vyšším počtem pozitivních hodnocení.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      features: { $type: "array", $ne: [] },
      positive_ratings: { $gt: 0, $type: ["int", "long", "double"] }
    }
  },
  {
    $project: {
      title: 1,
      publisher: 1,
      positive_ratings: 1,
      feature_count: { $size: "$features" },
      features_string: {
        $reduce: {
          input: "$features",
          initialValue: "",
          in: {
            $cond: [
              { $eq: ["$$value", ""] },
              "$$this",
              { $concat: ["$$value", " | ", "$$this"] }
            ]
          }
        }
      },
      feature_richness_score: {
        $round: [
          {
            $multiply: [
              { $size: "$features" },
              "$positive_ratings"
            ]
          },
          0
        ]
      }
    }
  },
  { $sort: { feature_richness_score: -1 } },
  { $limit: 15 },
  {
    $project: {
      title: 1,
      publisher: 1,
      positive_ratings: 1,
      feature_count: 1,
      features_string: 1,
      feature_richness_score: 1,
      _id: 0
    }
  }
]);


// --- Dotaz 17: $out – vytvoření kolekce steam_top_rated pro reporting ---
// Zadání: Materializuj výsledek komplexní pipeline do samostatné kolekce
// steam_top_rated pro rychlý reporting bez opakovaného výpočtu.
//
// Obecně: $out zapíše celý výsledek pipeline do nové nebo přepsané
// kolekce. Pipeline vybírá populární Steam hry a obohacuje je
// o odvozená pole total_ratings, approval_ratio a quality_tier.
//
// Konkrétně: Kolekce steam_top_rated bude obsahovat Steam hry
// s vyšším počtem pozitivních hodnocení, doplněné o reportingové metriky.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      positive_ratings: { $gt: 100, $type: ["int", "long", "double"] }
    }
  },
  {
    $addFields: {
      total_ratings: {
        $add: [
          "$positive_ratings",
          { $ifNull: ["$negative_ratings", 0] }
        ]
      },
      approval_ratio: {
        $round: [
          {
            $multiply: [
              {
                $divide: [
                  "$positive_ratings",
                  {
                    $add: [
                      "$positive_ratings",
                      { $ifNull: ["$negative_ratings", 0] }
                    ]
                  }
                ]
              },
              100
            ]
          },
          1
        ]
      },
      quality_tier: {
        $switch: {
          branches: [
            { case: { $gte: ["$positive_ratings", 50000] }, then: "Hit" },
            { case: { $gte: ["$positive_ratings", 10000] }, then: "Popular" },
            { case: { $gte: ["$positive_ratings", 1000] }, then: "Known" }
          ],
          default: "Niche"
        }
      }
    }
  },
  { $sort: { positive_ratings: -1 } },
  {
    $project: {
      title: 1,
      publisher: 1,
      release_year: 1,
      price: 1,
      positive_ratings: 1,
      negative_ratings: 1,
      total_ratings: 1,
      approval_ratio: 1,
      quality_tier: 1,
      genre: 1,
      _id: 0
    }
  },
  { $out: "steam_top_rated" }
]);

// Ověření výsledku:
db.getSiblingDB("gamesdb").steam_top_rated.aggregate([
  {
    $group: {
      _id: "$quality_tier",
      count: { $sum: 1 },
      avg_approval: { $avg: "$approval_ratio" }
    }
  },
  { $sort: { count: -1 } }
]);


// --- Dotaz 18: $setWindowFields + $merge – vytvoření kolekce s popularity_rank ---
// Zadání: Vytvoř odvozenou kolekci, ve které bude mít každá hra
// pole popularity_rank (pořadí dle positive_ratings v rámci platformy)
// a platform_total (celkový počet hodnocených her na dané platformě).
//
// Obecně: $setWindowFields s funkcí $rank() přiřadí pořadí v rámci
// jednotlivých partitions, zde podle source_platform. Současně se
// pomocí okenní agregace dopočítá pole platform_total. Výsledek se
// následně uloží pomocí $merge do nové kolekce, aby nebylo nutné
// přímo aktualizovat shardovanou zdrojovou kolekci.
//
// Konkrétně: Steam hra s nejvíce pozitivními hodnoceními dostane
// popularity_rank = 1, stejně tak nejvýše hodnocená Nintendo hra.
// Pořadí se počítá samostatně pro každou platformu.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      positive_ratings: { $type: ["int", "long", "double"], $gt: 0 },
      source_platform: { $exists: true, $ne: null }
    }
  },
  {
    $setWindowFields: {
      partitionBy: "$source_platform",
      sortBy: { positive_ratings: -1 },
      output: {
        popularity_rank: { $rank: {} },
        platform_total: {
          $sum: { $literal: 1 },
          window: { documents: ["unbounded", "unbounded"] }
        }
      }
    }
  },
  {
    $merge: {
      into: "games_ranked_by_popularity",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
]);

// Ověření výsledku:
db.getSiblingDB("gamesdb").games_ranked_by_popularity.find(
  { popularity_rank: { $lte: 3 } },
  {
    title: 1,
    source_platform: 1,
    popularity_rank: 1,
    positive_ratings: 1,
    platform_total: 1
  }
).sort({ source_platform: 1, popularity_rank: 1 });


// ==============================================================
// KATEGORIE 4: DISTRIBUCE DAT, CLUSTER, REPLIKACE A VÝPADEK
// ==============================================================

// --- Dotaz 19: Agregovaný přehled shardů, chunků a shard key hranic ---
// Zadání: Zobraz aktuální stav shardovaného clusteru pro kolekci
// gamesdb.games_unified_validated – pro každý shard zjisti počet chunků,
// hostitele a ukázkové hranice shard key.
//
// Obecně: Dotaz pracuje nad systémovými kolekcemi config.collections,
// config.chunks a config.shards. Nejprve získá UUID shardované kolekce,
// poté agreguje chunky podle shardu, přes $lookup doplní informace
// o hostitelích a v závěru zobrazí přehled distribuce chunků.
// Oproti příkazu sh.status() jde o cílený analytický dotaz nad metadata
// clusteru, který vrací strukturovaný výstup vhodný pro dokumentaci.
//
// Konkrétně: Výsledek ukazuje, na kterém shardu leží chunky kolekce
// games_unified_validated, kolik jich každý shard drží a jaké jsou
// ukázkové minimální a maximální hranice shard key.

const colMeta = db.getSiblingDB("config").collections.findOne({
  _id: "gamesdb.games_unified_validated"
});

const colUUID = colMeta ? colMeta.uuid : null;

if (!colUUID) {
  print("Kolekce gamesdb.games_unified_validated nebyla nalezena nebo není shardována.");
} else {
  db.getSiblingDB("config").chunks.aggregate([
    {
      $match: { uuid: colUUID }
    },
    {
      $group: {
        _id: "$shard",
        chunk_count: { $sum: 1 },
        min_keys: { $push: "$min" },
        max_keys: { $push: "$max" }
      }
    },
    {
      $lookup: {
        from: "shards",
        localField: "_id",
        foreignField: "_id",
        as: "shard_info"
      }
    },
    { $unwind: "$shard_info" },
    {
      $project: {
        shard: "$_id",
        chunk_count: 1,
        host: "$shard_info.host",
        sample_min_keys: { $slice: ["$min_keys", 2] },
        sample_max_keys: { $slice: ["$max_keys", 2] },
        _id: 0
      }
    },
    { $sort: { shard: 1 } }
  ]);
}


// --- Dotaz 20: Skutečná distribuce dokumentů podle shard key domény ---
// Zadání: Zjisti, kolik dokumentů a jaké procento dat připadá
// na jednotlivé oblasti shard key, které byly rozděleny mezi tři shardy.
// Ověř, zda je distribuce dat vyvážená, nebo zda je některá část
// clusteru zatížena výrazně více.
//
// Obecně: Dotaz pracuje nad hlavní kolekcí a agreguje dokumenty
// podle source_platform, což v tomto řešení odpovídá hlavním oblastem
// shard key a současně i rozdělení chunků mezi shardy. Pomocí $group
// se spočítá počet dokumentů, pomocí druhého $group celkový součet
// a následně se přes $project dopočítá procentuální podíl.
// Výsledkem je skutečné datové rozdělení, nikoli pouze metadata chunků.
//
// Konkrétně: Dotaz ukazuje, že i když metadata chunků mohou být
// rozdělena rovnoměrně, skutečný počet dokumentů mezi platformami
// (a tedy i mezi shard key oblastmi) může být výrazně nerovnoměrný.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $group: {
      _id: "$source_platform",
      doc_count: { $sum: 1 },
      rated_games: {
        $sum: {
          $cond: [
            { $gt: [{ $ifNull: ["$positive_ratings", 0] }, 0] },
            1,
            0
          ]
        }
      },
      scored_games: {
        $sum: {
          $cond: [
            { $gt: [{ $convert: { input: "$critic_score", to: "double", onError: 0, onNull: 0 } }, 0] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: null,
      total_docs: { $sum: "$doc_count" },
      rows: {
        $push: {
          platform: "$_id",
          doc_count: "$doc_count",
          rated_games: "$rated_games",
          scored_games: "$scored_games"
        }
      }
    }
  },
  { $unwind: "$rows" },
  {
    $project: {
      platform: "$rows.platform",
      doc_count: "$rows.doc_count",
      rated_games: "$rows.rated_games",
      scored_games: "$rows.scored_games",
      pct_of_cluster: {
        $round: [
          {
            $multiply: [
              { $divide: ["$rows.doc_count", "$total_docs"] },
              100
            ]
          },
          1
        ]
      },
      _id: 0
    }
  },
  { $sort: { doc_count: -1 } }
]);


// --- Dotaz 21: Stav replica set shard1RS – PRIMARY a replication lag ---
// Zadání: Zobraz stav všech uzlů replica setu shard1RS.
// Kdo je PRIMARY? Jaký je replication lag na secondary uzlech?
//
// Obecně: replSetGetStatus je admin příkaz ekvivalentní rs.status().
// Vrací pro každý člen replica setu: roli (PRIMARY/SECONDARY),
// zdraví (health: 1/0), uptime, datum heartbeatu a optimeDate
// (čas poslední zapsané operace). Skript pak vypočítá replication lag
// jako rozdíl optimeDate PRIMARY a každého SECONDARY uzlu v sekundách.
// Konkrétně: Spouštíme připojením přímo na uzel s1a (port 27018),
// protože replSetGetStatus není dostupný přes mongos router.
//
// Spuštění:
// docker exec s1a mongosh --port 27018 \
//   -u admin -p admin --authenticationDatabase admin \
//   --eval "
//     const s = db.adminCommand({ replSetGetStatus: 1 });
//     const primary = s.members.find(m => m.state === 1);
//     print('Set:', s.set, '| Members:', s.members.length);
//     s.members.forEach(m => {
//       const lag = primary ? ((primary.optimeDate - m.optimeDate) / 1000) : 'N/A';
//       print(m.name, '->', m.stateStr, '| health:', m.health,
//             '| lag_sec:', typeof lag === 'number' ? lag.toFixed(1) : lag);
//     });
//   "
//docker exec -it s1a mongosh --port 27018 -u admin -p admin --authenticationDatabase admin

const rsStatus = db.adminCommand({ replSetGetStatus: 1 });
const primaryMember = rsStatus.members.find(m => m.state === 1);
print("Replica set:", rsStatus.set, "| Celkem členů:", rsStatus.members.length);
rsStatus.members.forEach(m => {
  const lagSec = primaryMember
    ? ((primaryMember.optimeDate - m.optimeDate) / 1000).toFixed(1)
    : "N/A";
  print(m.name, "->", m.stateStr,
        "| health:", m.health,
        "| replication_lag_sec:", lagSec);
});


// --- Dotaz 22: Konfigurace replica set – priority, votes, election timeout ---
// Zadání: Zobraz konfiguraci replica setu shard1RS – priority uzlů,
// heartbeat timeout, election timeout a nastavení volebního procesu.
//
// Obecně: replSetGetConfig je admin příkaz ekvivalentní rs.conf().
// Vrací konfigurační dokument replica setu: members[] (host, priority,
// votes, hidden, arbiterOnly), settings (heartbeatTimeoutSecs,
// electionTimeoutMillis), protocolVersion a writeConcernMajorityJournalDefault.
// Ověřujeme, že žádný uzel není hidden ani arbiter a election timeout
// je dostatečně krátký pro rychlou volbu nového PRIMARY.
// Konkrétně: Spouštíme na s1a – očekáváme priority=1 a votes=1
// pro všechny tři uzly (s1a, s1b, s1c) a electionTimeoutMillis=10000.
//
// Spuštění:
// docker exec s1a mongosh --port 27018 \
//   -u admin -p admin --authenticationDatabase admin \
//   --eval "
//     const c = db.adminCommand({ replSetGetConfig: 1 }).config;
//     print('Set:', c._id, '| Protocol:', c.protocolVersion);
//     print('Election timeout ms:', c.settings.electionTimeoutMillis);
//     print('Heartbeat timeout s:', c.settings.heartbeatTimeoutSecs);
//     c.members.forEach(m => print(
//       m.host, '| priority:', m.priority, '| votes:', m.votes,
//       '| hidden:', m.hidden || false, '| arbiter:', m.arbiterOnly || false
//     ));
//   "
//docker exec -it s1a mongosh --port 27018 -u admin -p admin --authenticationDatabase admin
const rsConf = db.adminCommand({ replSetGetConfig: 1 }).config;
print("Set:", rsConf._id, "| Protocol:", rsConf.protocolVersion);
print("Election timeout ms:", rsConf.settings.electionTimeoutMillis);
print("Heartbeat timeout s:", rsConf.settings.heartbeatTimeoutSecs);
rsConf.members.forEach(m => print(
  m.host, "| priority:", m.priority, "| votes:", m.votes,
  "| hidden:", m.hidden || false, "| arbiter:", m.arbiterOnly || false
));


// --- Dotaz 23: Analýza distribuce dat a kvality shard key ---
// Zadání: Analyzuj rozložení dat podle shard key (source_platform)
// a současně vyhodnoť kvalitu dat (ratings, critic_score).
//
// Obecně: Dotaz využívá agregační pipeline, kde $group seskupuje
// dokumenty podle shard key a počítá jejich počet. Pomocí $cond
// a $sum se zároveň analyzuje kvalita dat (kolik dokumentů obsahuje
// ratingy a critic_score). Výsledkem je komplexní přehled distribuce
// dat a jejich úplnosti.
//
// Konkrétně: Dotaz ukazuje, zda jsou data rovnoměrně rozložena mezi
// platformy (Steam, PlayStation, Nintendo) a jaká je kvalita dat
// v jednotlivých shardech.

db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $group: {
      _id: "$source_platform",

      total_docs: { $sum: 1 },

      with_ratings: {
        $sum: {
          $cond: [
            { $gt: ["$positive_ratings", 0] },
            1,
            0
          ]
        }
      },

      with_critic: {
        $sum: {
          $cond: [
            { $gt: ["$critic_score", 0] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $addFields: {
      ratings_pct: {
        $round: [
          { $multiply: [
            { $divide: ["$with_ratings", "$total_docs"] },
            100
          ]},
          1
        ]
      },
      critic_pct: {
        $round: [
          { $multiply: [
            { $divide: ["$with_critic", "$total_docs"] },
            100
          ]},
          1
        ]
      }
    }
  },
  {
    $project: {
      platform: "$_id",
      total_docs: 1,
      ratings_pct: 1,
      critic_pct: 1,
      _id: 0
    }
  },
  { $sort: { total_docs: -1 } }
]);


// --- Dotaz 24: Simulace výpadku PRIMARY uzlu a automatická volba ---
// Zadání: Simuluj výpadek PRIMARY uzlu shard1RS (s1a), sleduj automatickou
// volbu nového PRIMARY (s1c) a ověř stav po obnovení.
//
// Obecně: MongoDB replica set používá Raft-based election protokol.
// Po výpadku PRIMARY zbývající uzly hlasují a nový PRIMARY je zvolen
// do ~10 sekund. Cluster je během volby krátkodobě nedostupný pro zápis
// (ale čtení z secondary pokračuje). Po opětovném spuštění se starý
// PRIMARY připojí zpět jako SECONDARY.
// Konkrétně: Skript zastaví s1a, počká 15s na volbu, zobrazí nový stav
// a poté s1a znovu spustí. Datová konzistence je zachována.

// Postup simulace (spustit v bash na serveru):
//   Krok 1 – stav před výpadkem:
//   docker exec s1a mongosh --port 27018 -u admin -p admin \
//     --authenticationDatabase admin \
//     --eval 'rs.status().members.forEach(m => print(m.name,"->",m.stateStr))'
//
//   Krok 2 – zastavení PRIMARY:
//   docker stop s1a
//
//   Krok 3 – stav po výpadku (nový PRIMARY s1c, ~10-15s po zastavení):
//   docker exec s1c mongosh --port 27018 -u admin -p admin \
//     --authenticationDatabase admin \
//     --eval 'rs.status().members.forEach(m => print(m.name,"->",m.stateStr))'
//
//   Krok 4 – obnovení:
//   docker start s1a && sleep 15
//   docker exec s1c mongosh --port 27018 -u admin -p admin \
//     --authenticationDatabase admin \
//     --eval 'rs.status().members.forEach(m => print(m.name,"->",m.stateStr))'
//
// Automatizovaný skript pokrývající všechny kroky:
//   bash scripts/simulate-failover.sh

// MongoDB dotaz – ověření konzistence dat po výpadku:
// Počet dokumentů na každém shardu NESMÍ klesnout ani po výpadku PRIMARY.
// Shardovaná kolekce čte z ostatních uzlů replica setu (SECONDARY),
// takže cluster zůstává dostupný pro čtení i během výpadku PRIMARY.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $group: {
      _id:        "$source_platform",
      doc_count:  { $sum: 1 },
      has_ratings:{ $sum: { $cond: [{ $gt: ["$positive_ratings", 0] }, 1, 0] } },
      has_scores: { $sum: { $cond: [{ $gt: ["$critic_score",    0] }, 1, 0] } }
    }
  },
  {
    $project: {
      platform:         "$_id",
      doc_count:        1,
      has_ratings:      1,
      has_scores:       1,
      data_completeness_pct: {
        $round: [
          { $multiply: [
            { $divide: [{ $add: ["$has_ratings", "$has_scores"] },
                         { $multiply: ["$doc_count", 2] }] },
            100
          ]}, 1
        ]
      },
      _id: 0
    }
  },
  { $sort: { platform: 1 } }
]);


// ==============================================================
// KATEGORIE 5: VALIDACE, INDEXY A FULLTEXTOVÉ VYHLEDÁVÁNÍ
// ==============================================================

// --- Dotaz 25: Audit kvality dat – detekce neúplných dokumentů ---
// Zadání: Najdi dokumenty s chybějícími nebo problematickými poli
// a sumarizuj kvalitu dat po platformách.
//
// Obecně: $project s kombinací $cond a $or umožňuje detekovat
// chybějící, nulové nebo logicky neplatné hodnoty. Následný $group
// agreguje počty problematických záznamů po platformách a $project
// převádí výsledky na procentuální ukazatele kvality dat.
//
// Konkrétně: Výsledek ukazuje, kolik procent záznamů má chybějící
// critic_score, price, release_year nebo genre v jednotlivých zdrojích.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $project: {
      source_platform: 1,
      missing_critic: {
        $cond: [{ $or: [
          { $eq: ["$critic_score", null] },
          { $not: [{ $gt: ["$critic_score", 0] }] }
        ]}, 1, 0]
      },
      missing_price: {
        $cond: [{ $or: [
          { $eq: ["$price", null] },
          { $lt: ["$price", 0] }
        ]}, 1, 0]
      },
      missing_year: {
        $cond: [{ $or: [
          { $eq: ["$release_year", null] },
          { $lt: ["$release_year", 1970] }
        ]}, 1, 0]
      },
      empty_genre: {
        $cond: [{ $or: [
          { $eq: ["$genre", null] },
          { $eq: [{ $size: { $ifNull: ["$genre", []] } }, 0] }
        ]}, 1, 0]
      }
    }
  },
  {
    $group: {
      _id:            "$source_platform",
      total:          { $sum: 1 },
      no_critic:      { $sum: "$missing_critic" },
      no_price:       { $sum: "$missing_price" },
      no_year:        { $sum: "$missing_year" },
      no_genre:       { $sum: "$empty_genre" }
    }
  },
  {
    $project: {
      platform:           "$_id",
      total:              1,
      missing_critic_pct: { $round: [{ $multiply: [{ $divide: ["$no_critic", "$total"] }, 100] }, 1] },
      missing_price_pct:  { $round: [{ $multiply: [{ $divide: ["$no_price",  "$total"] }, 100] }, 1] },
      missing_year_pct:   { $round: [{ $multiply: [{ $divide: ["$no_year",   "$total"] }, 100] }, 1] },
      missing_genre_pct:  { $round: [{ $multiply: [{ $divide: ["$no_genre",  "$total"] }, 100] }, 1] },
      _id: 0
    }
  },
  { $sort: { platform: 1 } }
]);


// --- Dotaz 26: Ověření validačního schématu – záměrně neplatný dokument ---
// Zadání: Ověř, že validační schéma skutečně odmítá neplatné dokumenty.
// Otestuj každé povinné pole a omezení hodnot (enum, minLength).
//
// Obecně: MongoDB JSON Schema validator (bsonType, required, enum)
// odmítne dokument nesplňující pravidla s WriteError kódem 121.
// try/catch zachytí chybu a zobrazí validační zprávu.
// Konkrétně: Testujeme že source_platform musí být steam/playstation/nintendo
// a title nesmí být prázdný řetězec.
try {
  db.getSiblingDB("gamesdb").games_unified_validated.insertOne({
    source_platform: "xbox",          // neplatná hodnota – není v enum
    source_id: 12345,
    title: "Test Game"
  });
  print("ERROR: Validace nepracuje správně – dokument byl přijat!");
} catch (e) {
  print("Validace funguje správně. Chyba:", e.message.substring(0, 120));
}

try {
  db.getSiblingDB("gamesdb").games_unified_validated.insertOne({
    source_platform: "steam",
    source_id: 99999,
    title: ""                          // prázdný string – porušení minLength:1
  });
  print("ERROR: Validace nepracuje správně – prázdný title byl přijat!");
} catch (e) {
  print("Validace minLength funguje. Chyba:", e.message.substring(0, 120));
}

// Zobrazení aktuálního validačního schématu:
db.getSiblingDB("gamesdb").runCommand({
  listCollections: 1,
  filter: { name: "games_unified_validated" }
}).cursor.firstBatch[0].options.validator;


// --- Dotaz 27: explain() – srovnání indexovaného dotazu a COLLSCAN ---
// Zadání: Porovnej plán vykonání dotazu nad polem release_year
// pro existující hodnotu v datech Steam her. Změř rozdíl mezi
// indexovaným přístupem a vynuceným full scanem.
//
// Obecně: explain("executionStats") vrací detailní plán dotazu
// včetně počtu prohlédnutých dokumentů, indexových klíčů a času.
// V prostředí mongos může být vrchní plán označen jako SINGLE_SHARD,
// ale klíčové jsou executionStats. Vynucený hint({ $natural: 1 })
// simuluje COLLSCAN bez použití indexu.
//
// Konkrétně: Dotaz ukazuje, že při vhodném indexu databáze prohlíží
// výrazně méně dokumentů než při full scan přístupu.

const explainResult = db.getSiblingDB("gamesdb").games_unified_validated.find(
  { release_year: 2018, source_platform: "steam" }
).explain("executionStats");

print("--- S indexem ---");
print("Nalezeno dokumentů:", explainResult.executionStats.nReturned);
print("Prohlédnuté dokumenty:", explainResult.executionStats.totalDocsExamined);
print("Prohlédnuté klíče:", explainResult.executionStats.totalKeysExamined);
print("Čas (ms):", explainResult.executionStats.executionTimeMillis);

const collscanResult = db.getSiblingDB("gamesdb").games_unified_validated.find(
  { release_year: 2018, source_platform: "steam" }
).hint({ $natural: 1 }).explain("executionStats");

print("\n--- Bez indexu (COLLSCAN) ---");
print("Nalezeno dokumentů:", collscanResult.executionStats.nReturned);
print("Prohlédnuté dokumenty:", collscanResult.executionStats.totalDocsExamined);
print("Prohlédnuté klíče:", collscanResult.executionStats.totalKeysExamined);
print("Čas (ms):", collscanResult.executionStats.executionTimeMillis);


// --- Dotaz 28: Partial index – vytvoření a ověření efektivity na Steam datech ---
// Zadání: Vytvoř partial index pro dražší a populární Steam hry
// (price > 30 a positive_ratings > 1000) a ověř, že dotaz na tuto
// podmnožinu dat index skutečně využívá.
//
// Obecně: Partial index indexuje jen dokumenty splňující
// partialFilterExpression. Je menší než plný index, snižuje režii
// při aktualizaci a je vhodný pro časté dotazy na vybranou podmnožinu.
// explain("executionStats") umožňuje ověřit, zda databáze použila
// indexovaný přístup místo plného průchodu kolekcí.
//
// Konkrétně: Dotaz ověřuje, že u Steam her s vyšší cenou a větším
// počtem pozitivních hodnocení bude použit partial index
// idx_popular_premium_steam.

db.getSiblingDB("gamesdb").games_unified_validated.createIndex(
  { source_platform: 1, price: -1, positive_ratings: -1 },
  {
    name: "idx_popular_premium_steam",
    partialFilterExpression: {
      source_platform: "steam",
      price: { $gt: 30 },
      positive_ratings: { $gt: 1000 }
    }
  }
);

const partialExplain = db.getSiblingDB("gamesdb").games_unified_validated.find(
  {
    source_platform: "steam",
    price: { $gt: 30 },
    positive_ratings: { $gt: 1000 }
  }
).hint("idx_popular_premium_steam").explain("executionStats");

print("--- S partial indexem ---");
print("Prohlédnuto dokumentů:", partialExplain.executionStats.totalDocsExamined);
print("Prohlédnuto klíčů:", partialExplain.executionStats.totalKeysExamined);
print("Vráceno dokumentů:", partialExplain.executionStats.nReturned);
print("Čas (ms):", partialExplain.executionStats.executionTimeMillis);
printjson(partialExplain.queryPlanner.winningPlan);

print("\n--- Ukázkové dokumenty ---");
db.getSiblingDB("gamesdb").games_unified_validated.find(
  {
    source_platform: "steam",
    price: { $gt: 30 },
    positive_ratings: { $gt: 1000 }
  },
  {
    title: 1,
    price: 1,
    positive_ratings: 1,
    source_platform: 1,
    _id: 0
  }
).hint("idx_popular_premium_steam").sort({ positive_ratings: -1 }).limit(10);


// --- Dotaz 29: $indexStats – analýza využití indexů v provozu ---
// Zadání: Které indexy jsou skutečně využívány dotazy?
// Identifikuj nepoužívané indexy, které zbytečně zabírají paměť.
//
// Obecně: $indexStats vrací pro každý index akumulované statistiky
// od posledního startu: accesses.ops (počet použití), accesses.since
// (datum od kdy se počítá). Index s ops=0 je kandidát na smazání.
// Konkrétně: Po spuštění předchozích dotazů v tomto souboru by indexy
// release_year, critic_score a text index měly mít ops > 0.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $indexStats: {} },
  {
    $group: {
      _id: "$name",
      key: { $first: "$key" },
      total_ops: { $sum: "$accesses.ops" },
      shards_present: { $sum: 1 },
      first_used: { $min: "$accesses.since" }
    }
  },
  {
    $project: {
      index_name: "$_id",
      key: 1,
      total_ops: 1,
      shards_present: 1,
      first_used: 1,
      is_unused: {
        $cond: [
          { $eq: ["$total_ops", 0] },
          true,
          false
        ]
      },
      _id: 0
    }
  },
  { $sort: { total_ops: -1 } }
]);


// --- Dotaz 30: $text search s textScore, frázovým dotazem a negací ---
// Zadání: Vyhledej hry tematicky spojené s fantasy světy, ale vyřaď
// hry obsahující slovo "online". Seřaď dle relevance (textScore)
// a porovnej s hodnocením kritiků.
//
// Obecně: $text operator využívá compound text index na polích
// title, publisher, developer (vytvořen v init-db.js).
// $meta: "textScore" vrátí skóre relevance pro každý dokument.
// Uvozovky = frázové vyhledávání, - = negace výrazu.
// Výsledek kombinuje fulltextovou relevanci s hodnocením kritiků
// přes compound $sort aby se ukázalo, kde se shodují i liší.
// Konkrétně: Hry jako Dragon Age, Dungeons & Dragons nebo
// Final Fantasy Fantasy by měly mít nejvyšší textScore.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      $text: { $search: "fantasy dragon magic -online" }
    }
  },
  {
    $project: {
      title:           1,
      publisher:       1,
      source_platform: 1,
      release_year:    1,
      price:           1,
      critic_score:    1,
      text_score:      { $meta: "textScore" }
    }
  },
  { $match: { text_score: { $gte: 1.0 } } },
  {
    $sort: { text_score: { $meta: "textScore" }, critic_score: -1 }
  },
  { $limit: 15 },
  {
    $project: {
      title:           1,
      publisher:       1,
      source_platform: 1,
      critic_score:    1,
      text_score:      { $round: ["$text_score", 2] },
      relevance_tier: {
        $switch: {
          branches: [
            { case: { $gte: ["$text_score", 3.0] }, then: "vysoce relevantní" },
            { case: { $gte: ["$text_score", 2.0] }, then: "relevantní" },
            { case: { $gte: ["$text_score", 1.0] }, then: "částečně relevantní" }
          ],
          default: "nízká relevance"
        }
      },
      _id: 0
    }
  }
]);
