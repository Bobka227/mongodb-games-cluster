// ============================================================
// DOTAZY - MongoDB Sharded Cluster (gamesdb)
// Kolekce: games_unified_validated
// Připojení přes mongos (port 27017)
// ============================================================
// Spuštění: mongosh --host localhost --port 27017 \
//           -u admin -p admin --authenticationDatabase admin \
//           dotazy.js
// ============================================================


// ============================================================
// KATEGORIE 1: PRÁCE S DATY (insert, update, delete, merge)
// ============================================================

// --- Dotaz 1.1: INSERT jednoho dokumentu ---
// Obecně: insertOne() vloží jeden dokument do kolekce.
// Konkrétně: Vkládáme novou hru "Cyberpunk 2077" pro platformu steam.
// Dokument musí projít validačním schématem (povinná pole: source_platform, source_id, title).
db.getSiblingDB("gamesdb").games_unified_validated.insertOne({
  source_platform: "steam",
  source_id: 1091500,
  title: "Cyberpunk 2077",
  publisher: "CD PROJEKT RED",
  developer: "CD PROJEKT RED",
  release_date: "2020-12-10",
  release_year: 2020,
  genre: ["RPG", "Action"],
  price: 59.99,
  critic_score: 86,
  user_score: 7.8,
  positive_ratings: 420000,
  negative_ratings: 85000,
  average_playtime: 9870,
  owners: "5,000,000 .. 10,000,000",
  features: ["Single-player", "Steam Achievements"],
  raw_source: { original_id: 1091500, import_batch: "manual_insert" }
});

// --- Dotaz 1.2: INSERT více dokumentů najednou ---
// Obecně: insertMany() vloží pole dokumentů v rámci jedné operace.
// Konkrétně: Přidáváme dvě hry pro platformu nintendo, které dosud v databázi chyběly.
db.getSiblingDB("gamesdb").games_unified_validated.insertMany([
  {
    source_platform: "nintendo",
    source_id: "HAC-P-AAAAA",
    title: "Metroid Prime Remastered",
    publisher: "Nintendo",
    developer: "Retro Studios",
    release_date: "2023-02-08",
    release_year: 2023,
    genre: ["Action", "Adventure"],
    price: 39.99,
    critic_score: 94,
    user_score: 9.1,
    positive_ratings: null,
    negative_ratings: null,
    average_playtime: null,
    owners: null,
    features: ["Single-player"],
    raw_source: { import_batch: "manual_insert" }
  },
  {
    source_platform: "nintendo",
    source_id: "HAC-P-BBBBB",
    title: "Fire Emblem Engage",
    publisher: "Nintendo",
    developer: "Intelligent Systems",
    release_date: "2023-01-20",
    release_year: 2023,
    genre: ["RPG", "Strategy"],
    price: 59.99,
    critic_score: 82,
    user_score: 8.3,
    positive_ratings: null,
    negative_ratings: null,
    average_playtime: null,
    owners: null,
    features: ["Single-player"],
    raw_source: { import_batch: "manual_insert" }
  }
]);

// --- Dotaz 1.3: UPDATE - aktualizace jednoho záznamu ($set) ---
// Obecně: updateOne() nalezne první dokument splňující filtr a aplikuje operaci $set.
// Konkrétně: Opravujeme cenu hry "Cyberpunk 2077" na Steamu po slevě a přidáváme pole discount.
db.getSiblingDB("gamesdb").games_unified_validated.updateOne(
  { source_platform: "steam", source_id: 1091500 },
  {
    $set: { price: 29.99 },
    $inc: { positive_ratings: 5000 }
  }
);

// --- Dotaz 1.4: UPDATE - hromadná aktualizace ($mul, $set) ---
// Obecně: updateMany() aktualizuje všechny dokumenty splňující podmínku.
// Konkrétně: Všem hrám na platformě nintendo vydaným před rokem 2020
// snižujeme cenu o 20% (multiplikátor 0.8) a označujeme je jako "legacy".
db.getSiblingDB("gamesdb").games_unified_validated.updateMany(
  { source_platform: "nintendo", release_year: { $lt: 2020 } },
  {
    $mul: { price: 0.8 },
    $set: { "raw_source.tag": "legacy" }
  }
);

// --- Dotaz 1.5: DELETE - mazání dokumentu s podmínkou ---
// Obecně: deleteOne() odstraní první dokument odpovídající filtru.
// Konkrétně: Mažeme testovací hru Fire Emblem Engage (source_id: "HAC-P-BBBBB"),
// kterou jsme vložili v dotazu 1.2 jako ukázku insertMany.
db.getSiblingDB("gamesdb").games_unified_validated.deleteOne(
  { source_platform: "nintendo", source_id: "HAC-P-BBBBB" }
);

// --- Dotaz 1.6: REPLACE (replaceOne) ---
// Obecně: replaceOne() nahradí celý dokument novým obsahem (kromě _id).
// Konkrétně: Nahrazujeme celý záznam hry Metroid Prime Remastered aktualizovanou verzí
// s doplněnými hodnoceními a opravenými metadaty.
db.getSiblingDB("gamesdb").games_unified_validated.replaceOne(
  { source_platform: "nintendo", source_id: "HAC-P-AAAAA" },
  {
    source_platform: "nintendo",
    source_id: "HAC-P-AAAAA",
    title: "Metroid Prime Remastered",
    publisher: "Nintendo",
    developer: "Retro Studios",
    release_date: "2023-02-08",
    release_year: 2023,
    genre: ["Action", "Adventure", "FPS"],
    price: 39.99,
    critic_score: 94,
    user_score: 9.2,
    positive_ratings: 18500,
    negative_ratings: 400,
    average_playtime: 1200,
    owners: "500,000 .. 1,000,000",
    features: ["Single-player", "HD Rumble"],
    raw_source: { import_batch: "manual_replace", verified: true }
  }
);

// --- Dotaz 1.7: BULK WRITE - kombinace operací v jednom volání ---
// Obecně: bulkWrite() umožňuje provést více různých operací (insert/update/delete)
// v rámci jednoho databázového volání, což snižuje latenci.
// Konkrétně: Vkládáme novou hru, aktualizujeme skóre existující hry
// a mažeme testovací záznam - vše najednou.
db.getSiblingDB("gamesdb").games_unified_validated.bulkWrite([
  {
    insertOne: {
      document: {
        source_platform: "playstation",
        source_id: "CUSA-99999",
        title: "Horizon Forbidden West Complete Edition",
        publisher: "Sony Interactive Entertainment",
        developer: "Guerrilla Games",
        release_date: "2023-04-21",
        release_year: 2023,
        genre: ["Action", "RPG", "Open World"],
        price: 59.99,
        critic_score: 90,
        user_score: 9.1,
        positive_ratings: null,
        negative_ratings: null,
        average_playtime: null,
        owners: null,
        features: ["Single-player", "PS5"],
        raw_source: { import_batch: "bulkwrite_demo" }
      }
    }
  },
  {
    updateOne: {
      filter: { source_platform: "steam", source_id: 1091500 },
      update: { $set: { "raw_source.bulk_verified": true } }
    }
  },
  {
    deleteOne: {
      filter: { source_platform: "playstation", source_id: "CUSA-99999" }
    }
  }
]);


// ============================================================
// KATEGORIE 2: AGREGAČNÍ FUNKCE
// ============================================================

// --- Dotaz 2.1: Průměrné hodnocení kritiků podle platformy (group + sort) ---
// Obecně: $group seskupuje dokumenty podle klíče, $avg počítá průměr,
// $sort řadí výsledky.
// Konkrétně: Vypočítáváme průměrné hodnocení kritiků pro každou platformu
// (steam/playstation/nintendo) a řadíme od nejlépe hodnocené.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { critic_score: { $type: ["int", "long", "double"], $gt: 0 } } },
  {
    $group: {
      _id: "$source_platform",
      avg_critic_score: { $avg: "$critic_score" },
      game_count: { $sum: 1 },
      max_score: { $max: "$critic_score" },
      min_score: { $min: "$critic_score" }
    }
  },
  { $sort: { avg_critic_score: -1 } },
  {
    $project: {
      platform: "$_id",
      avg_critic_score: { $round: ["$avg_critic_score", 2] },
      game_count: 1,
      max_score: 1,
      min_score: 1,
      _id: 0
    }
  }
]);

// --- Dotaz 2.2: Top 10 vydavatelů podle počtu her a průměrné ceně (group + sort + limit) ---
// Obecně: Pipeline nejprve filtruje hry s vydavatelem, poté seskupuje,
// počítá metriky a omezuje výstup na prvních 10 výsledků.
// Konkrétně: Hledáme vydavatele s největším portfoliem her a jejich průměrnou cenu.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { publisher: { $ne: null, $exists: true } } },
  {
    $group: {
      _id: "$publisher",
      total_games: { $sum: 1 },
      avg_price: { $avg: "$price" },
      platforms: { $addToSet: "$source_platform" }
    }
  },
  { $sort: { total_games: -1 } },
  { $limit: 10 },
  {
    $project: {
      publisher: "$_id",
      total_games: 1,
      avg_price: { $round: ["$avg_price", 2] },
      platform_count: { $size: "$platforms" },
      platforms: 1,
      _id: 0
    }
  }
]);

// --- Dotaz 2.3: Analýza žánrů pomocí $unwind (unwind + group + sort) ---
// Obecně: $unwind "rozbalí" pole - z jednoho dokumentu se žánry ["RPG","Action"]
// vzniknou dva dokumenty, každý s jedním žánrem. Poté je možné žánry agregovat.
// Konkrétně: Zjišťujeme popularitu žánrů podle počtu her a průměrného hodnocení kritiků.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { genre: { $type: "array", $ne: [] } } },
  { $unwind: "$genre" },
  {
    $group: {
      _id: "$genre",
      game_count: { $sum: 1 },
      avg_critic: { $avg: "$critic_score" },
      avg_price: { $avg: "$price" },
      platforms: { $addToSet: "$source_platform" }
    }
  },
  { $match: { game_count: { $gte: 10 } } },
  { $sort: { game_count: -1 } },
  { $limit: 15 },
  {
    $project: {
      genre: "$_id",
      game_count: 1,
      avg_critic: { $round: ["$avg_critic", 1] },
      avg_price: { $round: ["$avg_price", 2] },
      platform_count: { $size: "$platforms" },
      _id: 0
    }
  }
]);

// --- Dotaz 2.4: Roční trend vydávání her (group + sort + bucket) ---
// Obecně: $bucket rozděluje dokumenty do definovaných intervalů (bucketing).
// Konkrétně: Rozdělujeme hry do dekád (2000-2010, 2010-2015, 2015-2020, 2020+)
// a sledujeme trend vývoje herního průmyslu.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { release_year: { $gte: 2000, $lte: 2024, $type: ["int", "long", "double"] } } },
  {
    $bucket: {
      groupBy: "$release_year",
      boundaries: [2000, 2005, 2010, 2015, 2018, 2020, 2022, 2025],
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
      period: "$_id",
      game_count: 1,
      avg_price: { $round: ["$avg_price", 2] },
      avg_critic: { $round: ["$avg_critic", 1] },
      platform_count: { $size: "$platforms" },
      _id: 0
    }
  }
]);

// --- Dotaz 2.5: Hry s nejvyšším poměrem pozitivních hodnocení (aggregate + project + sort) ---
// Obecně: $project umožňuje přidávat computed fields pomocí $divide a $add.
// Konkrétně: Počítáme "approval ratio" (pozitivní / celkem hodnocení) pro Steam hry
// a hledáme nejlépe hodnocené hry komunitou.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      source_platform: "steam",
      positive_ratings: { $gt: 1000 },
      negative_ratings: { $gt: 0 }
    }
  },
  {
    $project: {
      title: 1,
      positive_ratings: 1,
      negative_ratings: 1,
      total_ratings: { $add: ["$positive_ratings", "$negative_ratings"] },
      approval_ratio: {
        $multiply: [
          { $divide: ["$positive_ratings", { $add: ["$positive_ratings", "$negative_ratings"] }] },
          100
        ]
      },
      price: 1
    }
  },
  { $match: { total_ratings: { $gte: 5000 } } },
  { $sort: { approval_ratio: -1 } },
  { $limit: 10 },
  {
    $project: {
      title: 1,
      total_ratings: 1,
      approval_ratio: { $round: ["$approval_ratio", 1] },
      price: 1,
      _id: 0
    }
  }
]);

// --- Dotaz 2.6: $facet - více agregací v jednom průchodu ---
// Obecně: $facet umožňuje spustit více nezávislých pipeline větví nad jedněmi daty.
// Konkrétně: V jednom dotazu získáváme distribuci cen (bucket), top žánry
// a přehled podle platformy - tři různé pohledy na data najednou.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { price: { $gt: 0 }, genre: { $type: "array" } } },
  {
    $facet: {
      price_distribution: [
        {
          $bucket: {
            groupBy: "$price",
            boundaries: [0, 5, 15, 30, 50, 70, 100],
            default: "100+",
            output: { count: { $sum: 1 }, avg_critic: { $avg: "$critic_score" } }
          }
        }
      ],
      top_genres: [
        { $unwind: "$genre" },
        { $group: { _id: "$genre", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 5 }
      ],
      by_platform: [
        {
          $group: {
            _id: "$source_platform",
            count: { $sum: 1 },
            avg_price: { $avg: "$price" }
          }
        }
      ]
    }
  }
]);

// --- Dotaz 2.7: $lookup - spojení s konfigurační kolekcí shardů ---
// Obecně: $lookup provádí levý outer join mezi dvěma kolekcemi, podobně jako SQL JOIN.
// Konkrétně: Spojujeme informace o hrách s metadaty o kolekcích z config.collections
// abychom zjistili, která platforma generuje nejvíce dat (size on disk per platform).
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $group: {
      _id: "$source_platform",
      game_count: { $sum: 1 },
      total_positive: { $sum: "$positive_ratings" },
      total_negative: { $sum: "$negative_ratings" }
    }
  },
  {
    $lookup: {
      from: { db: "config", coll: "shards" },
      pipeline: [
        { $project: { _id: 1, host: 1 } }
      ],
      as: "shard_info"
    }
  },
  {
    $project: {
      platform: "$_id",
      game_count: 1,
      total_positive: 1,
      total_negative: 1,
      shard_count: { $size: "$shard_info" },
      _id: 0
    }
  },
  { $sort: { game_count: -1 } }
]);

// --- Dotaz 2.8: $group + $push + $slice - výběr vzorků z každé skupiny ---
// Obecně: $push sbírá hodnoty do pole, $slice omezuje jeho délku.
// Konkrétně: Pro každý rok vydání sestavujeme seznam prvních 3 her (vzorek).
// Ukazuje strukturu vydávání her v čase s konkrétními příklady.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      release_year: { $gte: 2015, $lte: 2023, $type: ["int","long","double"] },
      critic_score: { $type: ["int","long","double"], $gte: 80 }
    }
  },
  { $sort: { critic_score: -1 } },
  {
    $group: {
      _id: "$release_year",
      top_games: { $push: { title: "$title", platform: "$source_platform", score: "$critic_score" } },
      total_count: { $sum: 1 },
      avg_score: { $avg: "$critic_score" }
    }
  },
  {
    $project: {
      year: "$_id",
      top_3_games: { $slice: ["$top_games", 3] },
      total_high_rated: "$total_count",
      avg_score: { $round: ["$avg_score", 1] },
      _id: 0
    }
  },
  { $sort: { year: -1 } }
]);


// ============================================================
// KATEGORIE 3: KONFIGURACE CLUSTERU A REPLICA SET
// ============================================================

// --- Dotaz 3.1: Stav shardovaného clusteru (sh.status()) ---
// Obecně: sh.status() zobrazuje přehled celého shardovaného clusteru: seznam shardů,
// databází, kolekcí, počty chunků a jejich rozmístění.
// Konkrétně: Ověřujeme, že kolekce games_unified_validated je správně shardována
// napříč třemi shardy (shard1RS, shard2RS, shard3RS).
sh.status();

// --- Dotaz 3.2: Stav replica set na primárním config serveru ---
// Obecně: rs.status() vrací podrobný stav všech členů replica set:
// PRIMARY/SECONDARY stav, zdraví, lag replikace, uptime.
// Konkrétně: Kontrolujeme stav konfiguračního replica setu cfgRS.
// Spustit na cfg1: mongosh --host cfg1 --port 27019
rs.status();

// --- Dotaz 3.3: Zobrazení konfigurace replica set ---
// Obecně: rs.conf() zobrazuje konfiguraci replica set: členy, priority, timeouty,
// heartbeat interval a další nastavení.
// Konkrétně: Zobrazujeme konfiguraci shard1RS pro ověření správného nastavení
// všech tří uzlů (s1a, s1b, s1c) s jejich prioritami.
// Spustit na s1a: mongosh --host s1a --port 27018
rs.conf();

// --- Dotaz 3.4: Přehled chunků v config databázi ---
// Obecně: config.chunks uchovává metadata o všech chunkcích v shardovaném clusteru.
// $group aggreguje počty chunků podle shardu a zobrazuje jejich rozmístění.
// Konkrétně: Zjišťujeme, kolik chunků leží na každém ze tří shardů
// pro kolekci gamesdb.games_unified_validated.
db.getSiblingDB("config").chunks.aggregate([
  {
    $match: {
      uuid: db.getSiblingDB("config").collections.findOne(
        { _id: "gamesdb.games_unified_validated" }
      )?.uuid
    }
  },
  {
    $group: {
      _id: "$shard",
      chunk_count: { $sum: 1 },
      chunks: { $push: { min: "$min", max: "$max" } }
    }
  },
  {
    $project: {
      shard: "$_id",
      chunk_count: 1,
      chunks: { $slice: ["$chunks", 3] },
      _id: 0
    }
  },
  { $sort: { shard: 1 } }
]);

// --- Dotaz 3.5: Replikační lag na secondary uzlech ---
// Obecně: rs.printSecondaryReplicationInfo() zobrazuje zpoždění replikace
// na každém sekundárním uzlu vůči primárnímu.
// Konkrétně: Měříme replication lag na shard1RS po provedení bulk operací.
// Spustit na s1a: mongosh --host s1a --port 27018
rs.printSecondaryReplicationInfo();

// --- Dotaz 3.6: Statistiky databáze a kolekcí ---
// Obecně: dbStats() vrací celkové statistiky databáze: velikost dat, indexů,
// počet kolekcí a dokumentů, storage engine info.
// Konkrétně: Zobrazujeme velikost gamesdb databáze distribuované přes shardy.
db.getSiblingDB("gamesdb").runCommand({ dbStats: 1, scale: 1024 });

// --- Dotaz 3.7: Detailní statistiky kolekce po shardech ---
// Obecně: collStats() s detailExecStats vrací statistiky kolekce včetně
// rozdělení dat mezi shardy.
// Konkrétně: Sledujeme jak jsou data games_unified_validated rozdělena
// mezi shard1RS, shard2RS a shard3RS.
db.getSiblingDB("gamesdb").runCommand({
  collStats: "games_unified_validated",
  scale: 1024
});

// --- Dotaz 3.8: Přehled operací balanceru ---
// Obecně: config.actionlog zaznamenává akce balanceru (přesuny chunků mezi shardy).
// Konkrétně: Zobrazujeme posledních 5 přesunů chunků, které balancer provedl
// při distribuci dat games_unified_validated.
db.getSiblingDB("config").actionlog.find(
  { what: "moveChunk.from" },
  { what: 1, ns: 1, details: 1, time: 1 }
).sort({ time: -1 }).limit(5);


// ============================================================
// KATEGORIE 4: NESTED (EMBEDDED) DOKUMENTY
// ============================================================

// --- Dotaz 4.1: Dotaz na pole uvnitř raw_source (tečkový zápis) ---
// Obecně: MongoDB umožňuje dotazovat se na vnořená pole pomocí "dot notation".
// Konkrétně: Hledáme hry, u nichž pole raw_source.import_batch odpovídá
// konkrétní hodnotě - filtrujeme záznamy importované jako "manual_insert".
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { "raw_source.import_batch": "manual_insert" },
  { title: 1, source_platform: 1, "raw_source.import_batch": 1, "raw_source.verified": 1 }
).pretty();

// --- Dotaz 4.2: Aktualizace vnořeného pole pomocí $set s tečkovou notací ---
// Obecně: $set s dot notation aktualizuje konkrétní pole uvnitř embedded dokumentu,
// aniž by přepsalo celý vnořený objekt.
// Konkrétně: Přidáváme metadata do raw_source pro všechny steam hry s cenou > 50:
// označujeme je jako "premium" a přidáváme datum kontroly kvality.
db.getSiblingDB("gamesdb").games_unified_validated.updateMany(
  { source_platform: "steam", price: { $gt: 50 } },
  {
    $set: {
      "raw_source.tier": "premium",
      "raw_source.qa_checked": true,
      "raw_source.qa_date": new Date().toISOString()
    }
  }
);
// Ověření výsledku:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { "raw_source.tier": "premium" },
  { title: 1, price: 1, "raw_source.tier": 1, "raw_source.qa_checked": 1 }
).limit(5);

// --- Dotaz 4.3: $elemMatch - podmínky na prvky pole ---
// Obecně: $elemMatch aplikuje více podmínek na prvky v poli.
// Bez $elemMatch by každá podmínka mohla platit pro jiný prvek pole.
// Konkrétně: Hledáme hry, které mají v poli features ZÁROVEŇ "Multi-player"
// I "Co-op" - tj. obě vlastnosti musí být přítomny.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  {
    $match: {
      features: { $all: ["Multi-player", "Co-op"] }
    }
  },
  {
    $project: {
      title: 1,
      source_platform: 1,
      features: 1,
      critic_score: 1,
      _id: 0
    }
  },
  { $sort: { critic_score: -1 } },
  { $limit: 10 }
]);

// --- Dotaz 4.4: $addToSet na poli features - přidání prvku bez duplikace ---
// Obecně: $addToSet přidá prvek do pole pouze pokud tam ještě není.
// Konkrétně: Hráčům Nintendo her přidáváme feature "Nintendo Switch Online"
// jako součást aktualizace metadat - bez rizika duplikátů.
db.getSiblingDB("gamesdb").games_unified_validated.updateMany(
  { source_platform: "nintendo" },
  { $addToSet: { features: "Nintendo Switch Online" } }
);
// Ověření:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { source_platform: "nintendo", features: "Nintendo Switch Online" },
  { title: 1, features: 1 }
).limit(5);

// --- Dotaz 4.5: $unset - odebrání vnořeného pole ---
// Obecně: $unset odstraní pole z dokumentu. S dot notation lze odstranit
// konkrétní vnořené pole bez dopadu na zbytek embedded dokumentu.
// Konkrétně: Odstraňujeme dočasné pole raw_source.tag u nintendo her,
// které jsme přidali v dotazu 1.4 jako testovací metadata.
db.getSiblingDB("gamesdb").games_unified_validated.updateMany(
  { source_platform: "nintendo", "raw_source.tag": "legacy" },
  { $unset: { "raw_source.tag": "" } }
);
// Ověření, že pole bylo odstraněno:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { source_platform: "nintendo", "raw_source.tag": { $exists: false } },
  { title: 1, raw_source: 1 }
).limit(5);

// --- Dotaz 4.6: Agregace podle obsahu pole genre ($in + unwind + group) ---
// Obecně: $in testuje přítomnost hodnoty v poli. Kombinace $match + $unwind + $group
// umožňuje analyzovat vztahy mezi žánry - co se nejčastěji kombinuje.
// Konkrétně: Pro hry obsahující žánr "RPG" zjišťujeme, s jakými dalšími žánry
// se nejčastěji kombinuje - analýza žánrových kombinací.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { genre: { $in: ["RPG"] } } },
  { $unwind: "$genre" },
  { $match: { genre: { $ne: "RPG" } } },
  {
    $group: {
      _id: "$genre",
      co_occurrence: { $sum: 1 },
      avg_critic: { $avg: "$critic_score" },
      platforms: { $addToSet: "$source_platform" }
    }
  },
  { $sort: { co_occurrence: -1 } },
  { $limit: 10 },
  {
    $project: {
      paired_genre: "$_id",
      co_occurrence: 1,
      avg_critic: { $round: ["$avg_critic", 1] },
      platforms: 1,
      _id: 0
    }
  }
]);


// ============================================================
// KATEGORIE 5: INDEXY
// ============================================================

// --- Dotaz 5.1: Zobrazení všech indexů kolekce ---
// Obecně: getIndexes() vrací seznam všech indexů kolekce včetně jejich typu,
// klíčů a options (unique, sparse, expireAfterSeconds, atd.).
// Konkrétně: Zobrazujeme 7 indexů vytvořených při inicializaci databáze:
// compound shard key index, title, release_year, publisher, genre, critic_score,
// user_score a text index.
db.getSiblingDB("gamesdb").games_unified_validated.getIndexes();

// --- Dotaz 5.2: Vytvoření partial indexu pro drahé hry ---
// Obecně: Partial index indexuje pouze dokumenty splňující zadaný filtr,
// čímž šetří místo a zrychluje operace pro specifické dotazy.
// Konkrétně: Vytváříme index pouze pro hry s cenou > 30 a hodnocením > 70,
// protože tyto hry jsou nejčastěji vyhledávány prémiových zákazníky.
db.getSiblingDB("gamesdb").games_unified_validated.createIndex(
  { price: -1, critic_score: -1 },
  {
    name: "idx_premium_games",
    partialFilterExpression: {
      price: { $gt: 30 },
      critic_score: { $gt: 70 }
    }
  }
);
// Ověření indexu:
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { price: { $gt: 30 }, critic_score: { $gt: 70 } }
).hint("idx_premium_games").limit(5).pretty();

// --- Dotaz 5.3: $text vyhledávání pomocí textového indexu ---
// Obecně: $text operator využívá textový index pro full-text vyhledávání.
// Vrací dokumenty dle relevance (textScore), podporuje frázové vyhledávání a negaci.
// Konkrétně: Vyhledáváme hry obsahující slova "fantasy" nebo "dragon" v názvu,
// vydavateli nebo vývojáři - využíváme compound text index z init-db.js.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $match: { $text: { $search: "fantasy dragon" } } },
  {
    $project: {
      title: 1,
      publisher: 1,
      source_platform: 1,
      critic_score: 1,
      score: { $meta: "textScore" },
      _id: 0
    }
  },
  { $sort: { score: { $meta: "textScore" } } },
  { $limit: 10 }
]);

// --- Dotaz 5.4: explain() - analýza plánu dotazu s indexem ---
// Obecně: explain("executionStats") zobrazuje detailní plán vykonání dotazu:
// jaký index byl použit (IXSCAN vs COLLSCAN), kolik dokumentů bylo prohlédnuto,
// počet klíčů indexu a čas vykonání v ms.
// Konkrétně: Analyzujeme efektivitu indexu na release_year pro dotaz
// hledající hry vydané v roce 2020 - porovnáváme IXSCAN vs COLLSCAN.
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { release_year: 2020, source_platform: "steam" }
).explain("executionStats");

// --- Dotaz 5.5: hint() - vynucení konkrétního indexu ---
// Obecně: hint() nutí MongoDB použít zadaný index místo toho,
// který by optimalizátor zvolil automaticky.
// Konkrétně: Porovnáváme výkon dotazu s vynuceným compound indexem
// {source_platform, source_id} (shard key index) vs. indexu na title.
db.getSiblingDB("gamesdb").games_unified_validated.find(
  { source_platform: "playstation", critic_score: { $gte: 85 } },
  { title: 1, critic_score: 1, source_platform: 1 }
).hint({ source_platform: 1, source_id: 1 }).sort({ critic_score: -1 }).limit(10);

// --- Dotaz 5.6: Statistiky využití indexů ---
// Obecně: $indexStats vrací statistiky o využití každého indexu:
// kolikrát byl použit od startu (nebo posledního restartu), kdy naposledy.
// Konkrétně: Zjišťujeme, které indexy jsou skutečně využívány při dotazování
// na games_unified_validated - pomáhá identifikovat nepoužívané indexy.
db.getSiblingDB("gamesdb").games_unified_validated.aggregate([
  { $indexStats: {} },
  {
    $project: {
      name: 1,
      key: 1,
      "accesses.ops": 1,
      "accesses.since": 1
    }
  },
  { $sort: { "accesses.ops": -1 } }
]);
