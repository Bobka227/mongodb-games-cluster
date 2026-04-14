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


// --- Dotaz 3: Vývoj průměrné ceny a hodnocení po dekádách s $bucket ---
// Zadání: Jak se vyvíjela průměrná cena her a hodnocení kritiků v čase?
// Rozděl hry do časových pásem a zjisti trend herního průmyslu.
//
// Obecně: $bucket rozdělí dokumenty do pevně definovaných intervalů
// podle release_year. $unwind rozbalí pole žánrů a $group uvnitř
// $facet počítá top žánr pro každou epochu. Kombinace $bucket +
// $facet umožňuje v jednom průchodu dat získat více různých pohledů.
// Konkrétně: Výsledek ukazuje, zda hry zdražují, zda roste nebo
// klesá průměrné hodnocení a jaký žánr dominuje v každé epoše.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      release_year: { $gte: 1995, $lte: 2024,
                      $type: ["int","long","double"] }
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
              game_count:  { $sum: 1 },
              avg_price:   { $avg: "$price" },
              avg_critic:  { $avg: "$critic_score" },
              platforms:   { $addToSet: "$source_platform" }
            }
          }
        },
        {
          $project: {
            epoch: "$_id", game_count: 1,
            avg_price:  { $round: ["$avg_price",  2] },
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
          $bucket: {
            groupBy: "$release_year",
            boundaries: [1995, 2005, 2015, 2025],
            default: "other",
            output: {
              genres: { $push: "$genre" }
            }
          }
        }
      ]
    }
  }
]);


// --- Dotaz 4: Vydavatelé s hrami napříč více platformami ---
// Zadání: Kteří vydavatelé vydali hry na všech třech platformách?
// Pro každého multi-platformního vydavatele zobraz jeho portfolio.
//
// Obecně: Pipeline používá $group s $addToSet pro sbírání unikátních
// platforem, $project s $size pro počítání, $match pro filtrování
// vydavatelů přítomných na 2+ platformách, $sort a $limit.
// $push sbírá vzorkové tituly do pole, $slice omezuje délku.
// Konkrétně: Ukazuje, kteří vydavatelé (např. EA, Ubisoft) jsou
// skutečně multi-platformní a kolik titulů mají na každé platformě.
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


// --- Dotaz 5: Korelační matice – cena vs hodnocení podle žánru ---
// Zadání: Existuje vztah mezi cenou hry a hodnocením kritiků?
// Analyzuj pro každý žánr zvlášť a zjisti, kde je korelace nejsilnější.
//
// Obecně: $unwind rozbalí pole žánrů, $group počítá agregáty potřebné
// pro výpočet korelačního koeficientu Pearson:
//   r = (n·Σxy − Σx·Σy) / sqrt((n·Σx² − (Σx)²)·(n·Σy² − (Σy)²))
// Pipeline pak v $project provede tento výpočet nad agregovanými
// hodnotami. Výsledek ukazuje skutečnou statistickou závislost.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      genre:        { $type: "array", $ne: [] },
      price:        { $gt: 0, $type: ["double","int","long"] },
      critic_score: { $gt: 0, $type: ["double","int","long"] }
    }
  },
  { $unwind: "$genre" },
  {
    $group: {
      _id:    "$genre",
      n:      { $sum: 1 },
      sum_x:  { $sum: "$price" },
      sum_y:  { $sum: "$critic_score" },
      sum_xy: { $sum: { $multiply: ["$price", "$critic_score"] } },
      sum_x2: { $sum: { $multiply: ["$price", "$price"] } },
      sum_y2: { $sum: { $multiply: ["$critic_score", "$critic_score"] } }
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
            num: { $subtract: [
              { $multiply: ["$n", "$sum_xy"] },
              { $multiply: ["$sum_x", "$sum_y"] }
            ]},
            den: { $sqrt: {
              $multiply: [
                { $subtract: [{ $multiply: ["$n","$sum_x2"] },
                              { $multiply: ["$sum_x","$sum_x"] }] },
                { $subtract: [{ $multiply: ["$n","$sum_y2"] },
                              { $multiply: ["$sum_y","$sum_y"] }] }
              ]
            }}
          },
          in: { $cond: [{ $eq: ["$$den", 0] }, null,
                        { $divide: ["$$num", "$$den"] }] }
        }
      },
      avg_price:  { $round: [{ $divide: ["$sum_x", "$n"] }, 2] },
      avg_critic: { $round: [{ $divide: ["$sum_y", "$n"] }, 1] },
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
// Obecně: $setWindowFields je operátor pro window funkce (MongoDB 5.0+).
// Umožňuje výpočty přes sousední dokumenty bez změny počtu výstupních
// dokumentů. Okno "range: [-1,1]" zahrnuje předchozí, aktuální a
// následující rok. Kombinace s $group (pro roční průměr) a $sort
// (pro správné okno) dává analýzu trendu s vyhlazením.
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
// Obecně: $merge zapíše výsledek pipeline do nové kolekce.
// Následný $lookup provede left outer join (ekvivalent SQL LEFT JOIN)
// mezi games_unified_validated a publishers_metadata podle pole publisher.
// $unwind s preserveNullAndEmptyArrays zachová hry bez vydavatele.
// Konkrétně: Každá hra bude obohacena o statistiky svého vydavatele
// (kolik má celkem her, na kolika platformách působí, avg cena).

// Krok A: vytvoření publishers_metadata
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null } } },
  {
    $group: {
      _id:           "$publisher",
      total_titles:  { $sum: 1 },
      platforms:     { $addToSet: "$source_platform" },
      avg_price:     { $avg: "$price" },
      avg_critic:    { $avg: "$critic_score" },
      genres:        { $addToSet: { $arrayElemAt: ["$genre", 0] } }
    }
  },
  {
    $project: {
      publisher_name:  "$_id",
      total_titles:    1,
      platform_count:  { $size: "$platforms" },
      platforms:       1,
      avg_price:       { $round: ["$avg_price",  2] },
      avg_critic:      { $round: ["$avg_critic", 1] },
      _id: 0
    }
  },
  { $merge: { into: "publishers_metadata", whenMatched: "replace" } }
]);

// Krok B: $lookup – spoj hry s metadaty vydavatele
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      price: { $gt: 20 },
      critic_score: { $gt: 70, $type: ["int","long","double"] }
    }
  },
  {
    $lookup: {
      from:         "publishers_metadata",
      localField:   "publisher",
      foreignField: "publisher_name",
      as:           "pub_stats"
    }
  },
  { $unwind: { path: "$pub_stats", preserveNullAndEmptyArrays: true } },
  {
    $project: {
      title: 1,
      price: 1,
      critic_score: 1,
      publisher: 1,
      "pub_stats.total_titles":   1,
      "pub_stats.platform_count": 1,
      "pub_stats.avg_critic":     1,
      score_vs_pub_avg: {
        $round: [{ $subtract: ["$critic_score",
                               { $ifNull: ["$pub_stats.avg_critic", 0] }] }, 1]
      },
      _id: 0
    }
  },
  { $sort: { score_vs_pub_avg: -1 } },
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
      source_platform: "steam",
      title: { $ne: null }
    }
  },
  {
    $addFields: {
      title_norm: { $toLower: { $trim: { input: "$title" } } }
    }
  },
  {
    $lookup: {
      from: "games_unified_validated",
      let:  { norm: "$title_norm", orig_platform: "$source_platform" },
      pipeline: [
        {
          $addFields: {
            title_norm: { $toLower: { $trim: { input: "$title" } } }
          }
        },
        {
          $match: {
            $expr: {
              $and: [
                { $eq:  ["$title_norm", "$$norm"] },
                { $ne:  ["$source_platform", "$$orig_platform"] }
              ]
            }
          }
        },
        {
          $project: {
            source_platform: 1,
            critic_score: 1,
            user_score: 1,
            price: 1,
            _id: 0
          }
        }
      ],
      as: "other_platforms"
    }
  },
  { $match: { "other_platforms.0": { $exists: true } } },
  {
    $project: {
      title: 1,
      steam_critic: "$critic_score",
      steam_price:  "$price",
      other_platforms: 1,
      platform_count: { $add: [1, { $size: "$other_platforms" }] },
      _id: 0
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


// --- Dotaz 10: $unionWith – srovnání Steam vs Nintendo cenových strategií ---
// Zadání: Porovnej distribuci cen Steam a Nintendo her v jednom výstupu.
// Spoj data obou platforem a zobraz srovnávací statistiky po cenových pásmech.
//
// Obecně: $unionWith spojí výsledky dvou pipeline do jednoho proudu
// dokumentů (podobně jako SQL UNION ALL). Umožňuje kombinovat data
// z různých filtrů nebo kolekcí. $bucket pak rozdělí spojená data
// do cenových pásem a $group podle platformy + pásma je porovná.
// Konkrétně: Ukazuje, zda Nintendo hry jsou průměrně dražší než Steam.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      price: { $gt: 0, $type: ["double","int","long"] }
    }
  },
  {
    $unionWith: {
      coll: "games_unified_validated",
      pipeline: [
        {
          $match: {
            source_platform: "nintendo",
            price: { $gt: 0, $type: ["double","int","long"] }
          }
        }
      ]
    }
  },
  {
    $bucket: {
      groupBy: "$price",
      boundaries: [0, 5, 15, 30, 60, 100],
      default: "100+",
      output: {
        count:        { $sum: 1 },
        avg_critic:   { $avg: "$critic_score" },
        platforms:    { $push: "$source_platform" }
      }
    }
  },
  {
    $project: {
      price_range:  "$_id",
      total_games:  "$count",
      avg_critic:   { $round: ["$avg_critic", 1] },
      steam_count: {
        $size: { $filter: { input: "$platforms",
                            cond: { $eq: ["$$this", "steam"] } } }
      },
      nintendo_count: {
        $size: { $filter: { input: "$platforms",
                            cond: { $eq: ["$$this", "nintendo"] } } }
      },
      _id: 0
    }
  }
]);


// --- Dotaz 11: $lookup s pipeline – top hry vydavatele a jeho průměr ---
// Zadání: Pro top 10 vydavatelů (podle počtu her) zobraz jejich nejlépe
// hodnocenou hru a porovnej ji s průměrem vydavatele.
//
// Obecně: $lookup s vnořenou pipeline umožňuje provést komplexní
// dotaz na spojovanou kolekci (filtrování, řazení, limit uvnitř lookup).
// Kombinace s $mergeObjects pak sloučí data z obou stran joinu.
// Konkrétně: Pro každého vydavatele je vidět, zda jeho "hit" výrazně
// překonává průměr portfolia (= publisher má jednu skvělou a jinak průměrné hry).
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null } } },
  {
    $group: {
      _id:        "$publisher",
      game_count: { $sum: 1 },
      avg_critic: { $avg: "$critic_score" }
    }
  },
  { $sort: { game_count: -1 } },
  { $limit: 10 },
  {
    $lookup: {
      from: "games_unified_validated",
      let:  { pub: "$_id" },
      pipeline: [
        { $match: { $expr: { $eq: ["$publisher", "$$pub"] },
                    critic_score: { $type: ["int","long","double"] } } },
        { $sort:  { critic_score: -1 } },
        { $limit: 1 },
        { $project: { title: 1, critic_score: 1,
                      source_platform: 1, _id: 0 } }
      ],
      as: "best_game"
    }
  },
  { $unwind: { path: "$best_game", preserveNullAndEmptyArrays: true } },
  {
    $project: {
      publisher:       "$_id",
      game_count:      1,
      avg_critic:      { $round: ["$avg_critic", 1] },
      best_title:      "$best_game.title",
      best_score:      "$best_game.critic_score",
      best_platform:   "$best_game.source_platform",
      score_above_avg: {
        $round: [{ $subtract: [
          { $ifNull: ["$best_game.critic_score", 0] },
          { $ifNull: ["$avg_critic", 0] }
        ]}, 1]
      },
      _id: 0
    }
  },
  { $sort: { score_above_avg: -1 } }
]);


// --- Dotaz 12: Obohacení her o tag kvality pomocí $lookup na publishers_metadata ---
// Zadání: Označ každou hru jako "flagship" pokud jde o nejlépe hodnocenou
// hru svého vydavatele a zároveň vydavatel působí na 2+ platformách.
//
// Obecně: Kombinuje $lookup (pro data vydavatele), $addFields s $cond
// pro podmíněné přiřazení tagu a $match pro filtraci. $project s
// $arrayElemAt přistupuje k prvnímu prvku výsledku lookup.
// Konkrétně: Výsledkem je seznam "flagship" titulů – her, které jsou
// vrcholem portfolia svého multi-platformního vydavatele.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      critic_score: { $gte: 85, $type: ["int","long","double"] },
      publisher:    { $ne: null }
    }
  },
  {
    $lookup: {
      from:         "publishers_metadata",
      localField:   "publisher",
      foreignField: "publisher_name",
      as:           "pub_meta"
    }
  },
  {
    $addFields: {
      pub_meta:       { $arrayElemAt: ["$pub_meta", 0] },
      is_flagship: {
        $cond: {
          if: {
            $and: [
              { $gte: ["$critic_score",
                       { $ifNull: [{ $arrayElemAt: ["$pub_meta.avg_critic", 0] }, 0] }] },
              { $gte: [{ $ifNull: [{ $arrayElemAt: ["$pub_meta.platform_count", 0] }, 0] }, 2] }
            ]
          },
          then: "flagship",
          else: "standard"
        }
      }
    }
  },
  { $match: { is_flagship: "flagship" } },
  {
    $project: {
      title: 1, source_platform: 1,
      critic_score: 1, publisher: 1,
      pub_total_titles:   "$pub_meta.total_titles",
      pub_platform_count: "$pub_meta.platform_count",
      pub_avg_critic:     "$pub_meta.avg_critic",
      is_flagship: 1,
      _id: 0
    }
  },
  { $sort: { critic_score: -1 } },
  { $limit: 20 }
]);


// ==============================================================
// KATEGORIE 3: TRANSFORMACE A OBOHACENÍ DAT
// ==============================================================

// --- Dotaz 13: Klasifikace her do tiers pomocí $switch + $addFields ---
// Zadání: Přiřaď každé hře kategorii kvality (AAA/AA/Indie/Unknown)
// na základě ceny a hodnocení, a cenový tier (Premium/Standard/Budget/Free).
// Vytvoř derivovanou kolekci pro rychlé filtrování.
//
// Obecně: $addFields přidává nová pole bez změny existujících.
// $switch je vícehodnotový podmíněný výraz (jako switch/case).
// $merge zapíše transformovaná data do nové kolekce – vytváří
// materializovaný pohled pro rychlejší dotazování.
// Konkrétně: Výsledná kolekce games_tiered umožní filtrovat hry
// bez opakovaného výpočtu klasifikace.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $addFields: {
      quality_tier: {
        $switch: {
          branches: [
            { case: { $and: [{ $gte: ["$critic_score", 85] },
                             { $gte: ["$price", 40] }] },
              then: "AAA" },
            { case: { $and: [{ $gte: ["$critic_score", 75] },
                             { $gte: ["$price", 20] }] },
              then: "AA" },
            { case: { $and: [{ $gte: ["$critic_score", 60] },
                             { $lt:  ["$price", 20] }] },
              then: "Indie" },
            { case: { $eq: ["$price", 0] },
              then: "Free-to-Play" }
          ],
          default: "Unknown"
        }
      },
      price_tier: {
        $switch: {
          branches: [
            { case: { $eq:  ["$price", 0] },      then: "Free" },
            { case: { $lte: ["$price", 9.99] },   then: "Budget" },
            { case: { $lte: ["$price", 29.99] },  then: "Standard" },
            { case: { $lte: ["$price", 59.99] },  then: "Premium" }
          ],
          default: "Luxury"
        }
      }
    }
  },
  {
    $group: {
      _id: { quality: "$quality_tier", price: "$price_tier" },
      count:       { $sum: 1 },
      avg_critic:  { $avg: "$critic_score" },
      avg_price:   { $avg: "$price" },
      platforms:   { $addToSet: "$source_platform" }
    }
  },
  { $sort: { "_id.quality": 1, "_id.price": 1 } },
  {
    $project: {
      quality_tier:  "$_id.quality",
      price_tier:    "$_id.price",
      game_count:    "$count",
      avg_critic:    { $round: ["$avg_critic", 1] },
      avg_price:     { $round: ["$avg_price",  2] },
      platforms:     1,
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


// --- Dotaz 16: $reduce – výpočet délky seznamu žánrů a genre score ---
// Zadání: Pro každou hru vypočítej "žánrovou šíři" (počet žánrů × průměrný
// critic_score) a najdi hry s nejlepším poměrem šíře žánrů ku hodnocení.
//
// Obecně: $reduce iteruje přes pole a akumuluje hodnotu (fold/reduce).
// Zde počítá délku žánrového pole alternativním způsobem a concatenuje
// žánry do řetězce. Kombinace s $multiply a $ifNull ukazuje komplexní
// odvozené pole v $project pipeline stage.
// Konkrétně: Hry s mnoha žánry a vysokým hodnocením jsou nejuniverzálnější.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      genre:        { $type: "array", $ne: [] },
      critic_score: { $gt: 70, $type: ["int","long","double"] }
    }
  },
  {
    $project: {
      title:        1,
      source_platform: 1,
      critic_score: 1,
      genre_count:  { $size: "$genre" },
      genre_string: {
        $reduce: {
          input:        "$genre",
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
      genre_breadth_score: {
        $round: [
          { $multiply: [
            { $size: "$genre" },
            "$critic_score"
          ]},
          0
        ]
      }
    }
  },
  { $sort: { genre_breadth_score: -1 } },
  { $limit: 15 },
  {
    $project: {
      title: 1,
      source_platform: 1,
      critic_score: 1,
      genre_count: 1,
      genre_string: 1,
      genre_breadth_score: 1,
      _id: 0
    }
  }
]);


// --- Dotaz 17: $out – vytvoření kolekce steam_top_rated pro reporting ---
// Zadání: Materializuj výsledek komplexní pipeline do samostatné kolekce
// steam_top_rated pro rychlý reporting bez opakovaného výpočtu.
//
// Obecně: $out zapíše celý výsledek pipeline do nové (nebo přepsané)
// kolekce. Na rozdíl od $merge (upsert) $out kolekci atomicky nahradí.
// Vhodné pro nočně obnovované reportovací pohledy (materialized views).
// Konkrétně: Kolekce steam_top_rated bude obsahovat jen Steam hry
// s hodnocením ≥ 80, obohacené o approval_ratio a quality_tier.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform:  "steam",
      critic_score:     { $gte: 80, $type: ["int","long","double"] },
      positive_ratings: { $gt: 100 }
    }
  },
  {
    $addFields: {
      total_ratings:  { $add: ["$positive_ratings", "$negative_ratings"] },
      approval_ratio: {
        $round: [
          { $multiply: [
            { $divide: ["$positive_ratings",
                         { $add: ["$positive_ratings",
                                  { $ifNull: ["$negative_ratings", 1] }] }] },
            100
          ]},
          1
        ]
      },
      quality_tier: {
        $switch: {
          branches: [
            { case: { $gte: ["$critic_score", 90] }, then: "Masterpiece" },
            { case: { $gte: ["$critic_score", 85] }, then: "Excellent" },
            { case: { $gte: ["$critic_score", 80] }, then: "Great" }
          ],
          default: "Good"
        }
      }
    }
  },
  { $sort: { critic_score: -1 } },
  {
    $project: {
      title: 1, publisher: 1, release_year: 1,
      price: 1, critic_score: 1, approval_ratio: 1,
      total_ratings: 1, quality_tier: 1, genre: 1
    }
  },
  { $out: "steam_top_rated" }
]);

// Ověření výsledku:
db.getSiblingDB("gamesdb").steam_top_rated.aggregate([
  { $group: { _id: "$quality_tier", count: { $sum: 1 }, avg_critic: { $avg: "$critic_score" } } },
  { $sort:  { avg_critic: -1 } }
]);


// --- Dotaz 18: Aktualizace dokumentů v pipeline – $merge s whenMatched ---
// Zadání: Přidej každé hře v kolekci pole popularity_rank (pořadí
// dle total_ratings v rámci platformy) pomocí pipeline a $merge.
//
// Obecně: $setWindowFields s $rank() přiřadí pořadí v rámci partitions
// (zde: platforma). $merge s whenMatched: "merge" pak aktualizuje
// existující dokumenty polem rank bez přepisování ostatních polí.
// Konkrétně: Steam hra s nejvíce hodnoceními dostane rank=1,
// nintendo hra s nejvíce hodnoceními dostane také rank=1 (samostatné pořadí).
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      positive_ratings: { $type: ["int","long","double"], $gt: 0 }
    }
  },
  {
    $setWindowFields: {
      partitionBy: "$source_platform",
      sortBy: { positive_ratings: -1 },
      output: {
        popularity_rank: { $rank: {} },
        platform_total:  {
          $sum: { $literal: 1 },
          window: { documents: ["unbounded","unbounded"] }
        }
      }
    }
  },
  {
    $project: {
      popularity_rank: 1,
      platform_total:  1
    }
  },
  {
    $merge: {
      into:         "games_unified_validated",
      whenMatched:  "merge",
      whenNotMatched: "discard"
    }
  }
]);

// Ověření:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { popularity_rank: { $lte: 3 } },
  { title: 1, source_platform: 1, popularity_rank: 1,
    positive_ratings: 1, platform_total: 1 }
).sort({ source_platform: 1, popularity_rank: 1 });


// ==============================================================
// KATEGORIE 4: DISTRIBUCE DAT, CLUSTER, REPLIKACE A VÝPADEK
// ==============================================================

// --- Dotaz 19: Detailní přehled shardů a vyvážení dat ---
// Zadání: Zobraz aktuální stav shardovaného clusteru – seznam shardů,
// počty chunků, hostitelé a zda je balancer aktivní.
//
// Obecně: sh.status() je příkaz mongos routeru agregující informace
// z config databáze o všech shardech, chunkcích a kolekcích.
// Vrací: seznam shardů (shard1RS, shard2RS, shard3RS) s hostiteli,
// konfiguraci balanceru, seznam shardovaných kolekcí a shard key.
// Konkrétně: Ověřujeme že kolekce gamesdb.games_unified_validated
// je shardována podle klíče {source_platform:1, source_id:1}.
sh.status();


// --- Dotaz 20: Počet dokumentů na každém shardu (getShardDistribution) ---
// Zadání: Kolik dokumentů a kolik % dat leží na každém ze tří shardů?
// Je distribuce dat vyvážená nebo je jeden shard přetížen?
//
// Obecně: getShardDistribution() vrací pro každý shard: počet dokumentů
// (count), velikost dat (size), počet chunků a odhadovaný průměrný
// počet dokumentů na chunk. Pomáhá identifikovat hot shards.
// Konkrétně: Distribuce by měla být přibližně 1/3 na každý shard.
// Nerovnoměrnost by indikovala nevhodnou volbu shard key.
db.getSiblingDB("gamesdb").games_unified_validated.getShardDistribution();


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
const rsConf = db.adminCommand({ replSetGetConfig: 1 }).config;
print("Set:", rsConf._id, "| Protocol:", rsConf.protocolVersion);
print("Election timeout ms:", rsConf.settings.electionTimeoutMillis);
print("Heartbeat timeout s:", rsConf.settings.heartbeatTimeoutSecs);
rsConf.members.forEach(m => print(
  m.host, "| priority:", m.priority, "| votes:", m.votes,
  "| hidden:", m.hidden || false, "| arbiter:", m.arbiterOnly || false
));


// --- Dotaz 23: Měření replication lag všech shardů z mongos ---
// Zadání: Změř zpoždění replikace na všech secondary uzlech ve všech
// třech shardech (shard1RS, shard2RS, shard3RS) z jednoho místa.
//
// Obecně: serverStatus().repl vrací informace o replikaci dostupné
// i přes mongos pro každý shard skrze db.adminCommand na příslušném
// primárním uzlu. db.getSiblingDB("config").shards.find() vrátí seznam
// shardů a jejich hostitelů. Kombinace obou přístupů dá přehled lagů
// napříč celým clusterem bez nutnosti přihlašovat se na každý shard zvlášť.
// Konkrétně: Pro dokumentaci spouštíme na s1a, s2a, s3a a porovnáváme
// výsledky – lag by měl být < 1s v lokálním Docker prostředí.
//
// Spuštění pro shard1RS (opakuj pro s2a, s3a):
// docker exec s1a mongosh --port 27018 \
//   -u admin -p admin --authenticationDatabase admin \
//   --eval "
//     const s = db.adminCommand({ replSetGetStatus: 1 });
//     const primary = s.members.find(m => m.state === 1);
//     const secondaries = s.members.filter(m => m.state === 2);
//     print('=== Shard:', s.set, '===');
//     print('PRIMARY:', primary.name, '| optime:', primary.optimeDate);
//     secondaries.forEach(sec => {
//       const lag = (primary.optimeDate - sec.optimeDate) / 1000;
//       print('SECONDARY:', sec.name,
//             '| syncedTo:', sec.optimeDate,
//             '| lag_sec:', lag.toFixed(2));
//     });
//   "

// Přehled shardů dostupný přes config databázi:
db.getSiblingDB("config").shards.aggregate([
  {
    $project: {
      shard_id:   "$_id",
      host:       1,
      state:      1,
      _id: 0
    }
  },
  { $sort: { shard_id: 1 } }
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
// Zadání: Najdi dokumenty s chybějícími kritickými poli, zkontroluj
// konzistenci datových typů a sumarizuj kvalitu dat po platformách.
//
// Obecně: $project s $type umožňuje kontrolu BSON typu pole.
// $cond + $or detekuje null, missing nebo nesprávný typ.
// Výsledný $group agreguje počty problematických záznamů.
// Tato pipeline implementuje data quality check bez externích nástrojů.
// Konkrétně: Ukazuje kolik % záznamů má null critic_score, kolik
// má nesprávný typ source_id a kolik chybí release_year.
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


// --- Dotaz 27: explain() – srovnání IXSCAN vs COLLSCAN ---
// Zadání: Porovnej plány dotazu pro filtrování podle release_year:
// jednou bez indexu (COLLSCAN), jednou s indexem (IXSCAN).
// Změř rozdíl v počtu prohlédnutých dokumentů.
//
// Obecně: explain("executionStats") zobrazuje detailní plán vykonání:
// - COLLSCAN: prochází celou kolekci (totalDocsExamined = vše)
// - IXSCAN: prochází jen odpovídající záznamy v indexu
// Klíčové metriky: totalDocsExamined, totalKeysExamined, executionTimeMillis.
// Konkrétně: Index na release_year byl vytvořen v init-db.js,
// dotaz by měl použít IXSCAN a prohlédnout jen ~2000 dokumentů.
const explainResult = db.getSiblingDB("gamesdb").games_unified_validated.find(
  { release_year: 2020, source_platform: "steam" }
).explain("executionStats");

print("Použitý plán:", explainResult.queryPlanner.winningPlan.stage);
print("Prohlédnuté dokumenty:", explainResult.executionStats.totalDocsExamined);
print("Vrácené dokumenty:",     explainResult.executionStats.nReturned);
print("Čas (ms):",              explainResult.executionStats.executionTimeMillis);

// Srovnání bez indexu (forced COLLSCAN):
const collscanResult = db.getSiblingDB("gamesdb").games_unified_validated.find(
  { release_year: 2020, source_platform: "steam" }
).hint({ $natural: 1 }).explain("executionStats");

print("\n--- Bez indexu (COLLSCAN) ---");
print("Prohlédnuté dokumenty:", collscanResult.executionStats.totalDocsExamined);
print("Čas (ms):",              collscanResult.executionStats.executionTimeMillis);


// --- Dotaz 28: Partial index – vytvoření a ověření efektivity ---
// Zadání: Vytvoř partial index jen pro prémiové hry (price > 30 a critic_score > 80).
// Ověř, že dotaz na prémiové hry tento index skutečně využívá.
//
// Obecně: Partial index indexuje jen podmnožinu dokumentů splňující
// partialFilterExpression. Je menší než plný index, rychleji se
// aktualizuje a šetří RAM. Vhodný pro frekventované dotazy na podmnožinu.
// Konkrétně: Dotazy na prémiové Steam hry budou rychlejší, ostatní
// dotazy na celou kolekci budou používat jiné indexy.
db.getSiblingDB("gamesdb").games_unified_validated.createIndex(
  { price: -1, critic_score: -1, source_platform: 1 },
  {
    name: "idx_premium_titles",
    partialFilterExpression: {
      price:        { $gt: 30 },
      critic_score: { $gt: 80 }
    }
  }
);

// Ověření využití partial indexu:
const partialExplain = db.getSiblingDB("gamesdb").games_unified_validated.find(
  { price: { $gt: 30 }, critic_score: { $gt: 80 }, source_platform: "steam" }
).explain("executionStats");

print("Plán dotazu:", JSON.stringify(partialExplain.queryPlanner.winningPlan, null, 2));
print("Prohlédnuto dokumentů:", partialExplain.executionStats.totalDocsExamined);

// Vrácení výsledku dotazu:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { price: { $gt: 30 }, critic_score: { $gt: 80 } },
  { title: 1, price: 1, critic_score: 1, source_platform: 1 }
).hint("idx_premium_titles").sort({ critic_score: -1 }).limit(10);


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
    $project: {
      index_name:   "$name",
      key:          "$key",
      ops_count:    "$accesses.ops",
      first_used:   "$accesses.since",
      is_unused:    { $cond: [{ $eq: ["$accesses.ops", 0] }, true, false] }
    }
  },
  { $sort: { ops_count: -1 } }
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
