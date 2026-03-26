  const gdb = db.getSiblingDB("gamesdb");

try {
  sh.enableSharding("gamesdb");
  print("Sharding enabled for gamesdb");
} catch (e) {
  print("gamesdb sharding already enabled or skipped: " + e);
}

const validatorSchema = {
  $jsonSchema: {
    bsonType: "object",
    required: ["source_platform", "source_id", "title"],
    properties: {
      source_platform: {
        bsonType: "string",
        enum: ["steam", "playstation", "nintendo"]
      },
      source_id: {
        bsonType: ["int", "long", "double", "string", "null"]
      },
      title: {
        bsonType: "string",
        minLength: 1
      },
      publisher: {
        bsonType: ["string", "null"]
      },
      developer: {
        bsonType: ["string", "null"]
      },
      release_date: {
        bsonType: ["string", "null"]
      },
      release_year: {
        bsonType: ["int", "long", "double", "null"]
      },
      genre: {
        bsonType: ["array", "null"],
        items: { bsonType: "string" }
      },
      price: {
        bsonType: ["double", "int", "long", "null"]
      },
      critic_score: {
        bsonType: ["double", "int", "long", "string", "null"]
      },
      user_score: {
        bsonType: ["double", "int", "long", "string", "null"]
      },
      positive_ratings: {
        bsonType: ["int", "long", "double", "null"]
      },
      negative_ratings: {
        bsonType: ["int", "long", "double", "null"]
      },
      average_playtime: {
        bsonType: ["int", "long", "double", "null"]
      },
      owners: {
        bsonType: ["string", "null"]
      },
      features: {
        bsonType: ["array", "null"],
        items: { bsonType: "string" }
      },
      raw_source: {
        bsonType: ["object", "null"]
      }
    }
  }
};

const collExists = gdb.getCollectionNames().includes("games_unified_validated");

if (!collExists) {
  gdb.createCollection("games_unified_validated", {
    validator: validatorSchema,
    validationLevel: "strict",
    validationAction: "error"
  });
  print("Created games_unified_validated with validator");
} else {
  gdb.runCommand({
    collMod: "games_unified_validated",
    validator: validatorSchema,
    validationLevel: "strict",
    validationAction: "error"
  });
  print("Updated validator for games_unified_validated");
}

try {
  sh.shardCollection("gamesdb.games_unified_validated", { source_platform: 1, source_id: 1 });
  print("Collection sharded");
} catch (e) {
  print("Collection already sharded or skipped: " + e);
}

gdb.games_unified_validated.createIndex({ source_platform: 1, source_id: 1 });
gdb.games_unified_validated.createIndex({ title: 1 });
gdb.games_unified_validated.createIndex({ release_year: 1 });
gdb.games_unified_validated.createIndex({ publisher: 1 });
gdb.games_unified_validated.createIndex({ genre: 1 });
gdb.games_unified_validated.createIndex({ critic_score: 1 });
gdb.games_unified_validated.createIndex({ user_score: 1 });
gdb.games_unified_validated.createIndex({ title: "text", publisher: "text", developer: "text" });

print("Indexes created");