# Vysvětlení dotazů pro obhajobu

## Kategorie 1: Agregační a analytické dotazy (Q1–Q6)

### Q1 — Multidimenzionální srovnání platforem
> "Dotaz pomocí $group seskupí všechny hry podle platformy a pro každou vypočítá agregované metriky — počet her, průměrnou cenu, průměrné hodnocení kritiků a celkový počet hodnocení. $cond umožňuje podmíněný součet — počítám jen hry vydané po roce 2018 pro výpočet modern_game_ratio. $ifNull ošetřuje chybějící hodnoty aby součet neskončil jako null. Výsledek ukazuje která platforma má nejmodernější katalog a nejlepší průměrné hodnocení."

---

### Q2 — Wilson Score
> "Dotaz seřadí Steam hry podle statisticky spolehlivého hodnocení — Wilson lower bound při 95% spolehlivosti. Prostý průměr není spolehlivý — hra se 10 hodnoceními a 100% pozitivními by byla výše než hra s 50 000 hodnoceními a 95% pozitivními. Wilson score zohledňuje nejistotu při malém počtu hodnocení. Celý výpočet probíhá přímo v MongoDB pipeline pomocí $sqrt, $divide, $multiply a $let pro pojmenování mezivýsledků — bez exportu dat."

---

### Q3 — Vývoj cen a top žánry podle epoch
> "Dotaz používá $facet pro dva paralelní analýzy nad stejnými daty. První větev $bucket rozděluje hry do časových košů a počítá průměrnou cenu a hodnocení v každém období. Druhá větev $unwind rozloží pole žánrů — jedna hra s pěti žánry se stane pěti dokumenty — a pomocí dvojitého $group a $slice najde top 5 žánrů v každé epoše. Klíčová výhoda $facet je jeden průchod daty místo dvou samostatných dotazů."

---

### Q4 — Vydavatelé na více platformách
> "Dotaz pomocí $group s $addToSet sbírá unikátní platformy každého vydavatele do pole. $size pak spočítá počet platforem a druhý $match filtruje pouze vydavatele přítomné alespoň na dvou platformách. Zajímavé je že filtrujeme podle odvozeného pole platform_count které vzniklo až během pipeline — v SQL by to vyžadovalo poddotaz nebo HAVING."

---

### Q5 — Pearsonova korelace (cena vs herní doba)
> "Dotaz počítá Pearsonův korelační koeficient mezi cenou hry a průměrnou herní dobou zvlášť pro každý žánr. $unwind nejprve rozloží žánry, $group pak pro každý žánr akumuluje součty potřebné pro vzorec — sum_x, sum_y, sum_xy, sum_x2, sum_y2. $project následně aplikuje vzorec přes $let, $sqrt a $divide. Výsledek říká ve kterých žánrech mají dražší hry tendenci být delší — například RPG versus casual hry."

---

### Q6 — Klouzavý průměr hodnocení ($setWindowFields)
> "Dotaz nejprve $group agreguje Nintendo hry na roční průměry hodnocení kritiků. Pak $setWindowFields aplikuje okenní funkce — window: range [-1, 1] znamená že pro každý rok vezme předchozí, aktuální a následující rok a spočítá klouzavý průměr. Tím se odfiltruje šum způsobený roky s malým počtem her. Současně se pomocí documents: unbounded počítá kumulativní součet her od začátku časové řady."

---

## Kategorie 2: Propojování dat a vazby mezi datasety (Q7–Q12)

### Q7 — $merge + $lookup (metadata vydavatelů)
> "Dotaz je třístupňový. Nejprve se smaže stará pomocná kolekce a vytvoří se unique index. Ve druhém kroku pipeline agreguje statistiky vydavatelů a $merge je uloží do nové kolekce publishers_metadata — podobně jako materializovaný pohled. Ve třetím kroku $lookup propojí herní záznamy s touto kolekcí a přidá k nim statistiky vydavatele. Výsledek ukazuje dražší Steam hry obohacené o informace o tom jak velký a multi-platformní jejich vydavatel je."

---

### Q8 — Hry na více platformách (self-join)
> "Dotaz normalizuje názvy her přes $toLower a $trim aby se shodovaly například 'FIFA 22' a 'fifa 22'. Pak $group seskupí záznamy podle normalizovaného názvu a $addToSet sbírá platformy. $match pak filtruje pouze skupiny kde je přítomný Steam a zároveň více než jedna platforma. Jde o self-join — kolekce se spojuje sama se sebou podle názvu hry."

---

### Q9 — Distribuce chunků ($lookup na config)
> "Dotaz čte systémové kolekce MongoDB — config.collections pro získání UUID shardované kolekce a config.chunks pro metadata o rozdělení dat. $group spočítá chunky per shard a $lookup obohacuje výsledek o hostname shardu z config.shards. Oproti příkazu sh.status() jde o strukturovaný analytický dotaz nad metadaty clusteru vhodný pro dokumentaci."

---

### Q10 — $unionWith (levné vs drahé hry)
> "Dotaz používá $unionWith — MongoDB ekvivalent SQL UNION. Nejprve zpracuje levné Steam hry do 10 dolarů a označí je price_group: cheap. $unionWith pak připojí druhý proud — drahé hry nad 30 dolarů označené jako expensive. Oba proudy se sloučí a $group spočítá průměrný počet hodnocení pro každou skupinu. Výsledek ukazuje zda dražší hry mají více hodnocení."

---

### Q11 — $lookup s pipeline (nejlepší hra vydavatele)
> "Dotaz nejprve najde top vydavatele podle počtu her. Pak $lookup s vnořenou pipeline pro každého vydavatele samostatně dohledá jeho nejlépe hodnocenou hru — uvnitř lookup pipeline je $sort a $limit: 1. Výsledný $project vypočítá score_above_avg — o kolik bodů je nejlepší titul nad průměrem portfolia. Ukazuje zda má vydavatel vyrovnané portfolio nebo jeden výjimečný titul."

---

### Q12 — Flagship tituly vydavatelů
> "Dotaz je dvoustupňový. Nejprve $merge uloží statistiky vydavatelů do pomocné kolekce — maximální critic_score a počet platforem. Ve druhém kroku $lookup propojí hry s touto kolekcí a $match s $expr filtruje pouze hry kde critic_score hry se rovná maximálnímu skóre vydavatele. $expr umožňuje porovnávat pole dokumentu s polem z joinu — to standardní $match neumí."

---

## Kategorie 3: Transformace a obohacení dat (Q13–Q18)

### Q13 — Quality a price tiery ($switch)
> "Dotaz přiřadí každé hře dvě kategorie pomocí $switch — quality_tier podle critic_score a price_tier podle ceny. $switch je vícevětvové if-else přímo v pipeline. Pak $group agreguje hry podle kombinace obou tierů. Výsledkem je matice která ukazuje například kolik her je zároveň Excellent kvality a Standard ceny a na kterých platformách se tato kombinace vyskytuje."

---

### Q14 — $replaceRoot + $mergeObjects (vyrovnání raw_source)
> "Dotaz použije $replaceRoot k nahrazení kořene dokumentu novým objektem vytvořeným přes $mergeObjects. $mergeObjects sloučí hlavní pole dokumentu s obsahem vnořeného raw_source do jednoho plochého dokumentu. Místo game.raw_source.achievements bude přímo game.achievements. Výsledek je vhodný pro export nebo reporting kde vnořená struktura překáží."

---

### Q15 — $map + $filter + $regexMatch
> "Dotaz nejprve $filter odfiltruje z pole features pouze multiplayer položky — ty které obsahují slova Multi, Co-op nebo Online testované přes $regexMatch. $map pak každou položku převede na lowercase. $size spočítá kolik multiplayer features hra má a $group agreguje hry podle tohoto počtu. Ukazuje kolik Steam her podporuje 1, 2 nebo 3 a více multiplayer funkcí."

---

### Q16 — $reduce (features do řetězce)
> "Dotaz použije $reduce — funkci která iteruje přes pole a akumuluje výsledek. Zde skládá pole features do jednoho textového řetězce odděleného svislítkem. Zároveň vypočítá feature_richness_score jako počet features krát počet pozitivních hodnocení — kombinovaný ukazatel bohatosti funkcí a popularity. $reduce je jako fold nebo reduce z funkcionálního programování."

---

### Q17 — $out (materializovaný výsledek)
> "Dotaz zpracuje Steam hry s více než 100 hodnoceními, přidá odvozená pole total_ratings, approval_ratio a quality_tier pomocí $switch a celý výsledek zapíše přes $out do nové kolekce steam_top_rated. $out přepíše kolekci atomicky — buď se zapíše celá nebo vůbec. Výhodou je že reportingové dotazy pak čtou z malé předpočítané kolekce místo procházení všech 38 801 dokumentů."

---

### Q18 — $setWindowFields + $merge (popularity rank)
> "Dotaz přiřadí každé hře pořadí popularity v rámci její platformy. $setWindowFields s $rank() a partitionBy: source_platform počítá rank samostatně pro každou platformu — Steam hra s nejvíce hodnoceními dostane rank 1, stejně tak nejpopulárnější Nintendo hra. $merge pak výsledek uloží do nové kolekce games_ranked_by_popularity aniž by měnil shardovanou zdrojovou kolekci."

---

## Kategorie 4: Distribuce dat, cluster a replikace (Q19–Q24)

### Q19 — Metadata chunků clusteru
> "Dotaz analyzuje metadata shardovaného clusteru. Nejprve načte UUID kolekce z config.collections, pak agreguje chunky z config.chunks podle shardu a přes $lookup doplní hostname z config.shards. Výsledkem je strukturovaný přehled kolik chunků leží na každém shardu a jaké jsou hranice shard key — analytická alternativa k příkazu sh.status()."

---

### Q20 — Reálné rozdělení dokumentů
> "Dotaz pomocí dvojitého $group zjistí skutečný počet dokumentů na každém shardu. První $group počítá per platformu, druhý $group spočítá celkový součet a $project dopočítá procentuální podíl. Výsledek ukazuje reálnou nerovnoměrnost — Steam tvoří 70% dat, shard3RS je tedy výrazně více zatížen než ostatní shardy."

---

### Q21 — Stav replica setu (replication lag)
> "Dotaz spouštíme přímo na uzlu s1a přes docker exec, protože replSetGetStatus není dostupný přes mongos router. Admin příkaz vrátí stav všech členů replica setu — kdo je PRIMARY, kdo SECONDARY, zdraví uzlů a optimeDate. Z rozdílu optimeDate PRIMARY a SECONDARY vypočítáme replication lag v sekundách — zpoždění s jakým se secondary synchronizují."

---

### Q22 — Konfigurace replica setu
> "Dotaz replSetGetConfig vrátí konfigurační dokument replica setu. Zobrazí priority a votes každého uzlu — ověříme že všechny tři uzly mají priority 1 a votes 1 tedy rovnocenné hlasování. electionTimeoutMillis ukazuje jak rychle proběhne volba nového PRIMARY po výpadku — výchozí hodnota je 10 000 milisekund tedy 10 sekund."

---

### Q23 — Kvalita dat po shardech
> "Dotaz $group s $cond/$sum počítá pro každou platformu kolik dokumentů má vyplněné positive_ratings a kolik critic_score. $addFields pak dopočítá procentuální podíl. Výsledek kvantifikuje heterogenitu datasetu — Steam má ratings ale ne critic_score, Nintendo má critic_score ale ne ratings, PlayStation nemá ani jedno."

---

### Q24 — Simulace failover
> "Dotaz ověřuje konzistenci dat po výpadku uzlu. Pipeline počítá dokumenty per platforma a data_completeness_pct — podíl dokumentů s hodnoceními. Spouštíme ho před a po zastavení uzlů. Pokud se čísla nemění, data jsou konzistentní. To demonstruje CP chování clusteru — systém raději odmítne zápisy než by riskoval ztrátu dat."

---

## Kategorie 5: Validace, indexy a fulltextové vyhledávání (Q25–Q30)

### Q25 — Audit kvality dat
> "Dotaz detekuje chybějící nebo problematická pole v každém dokumentu. $project s $cond a $or označí dokument příznakem 1 pokud má null critic_score, zápornou cenu, rok před 1970 nebo prázdné pole žánrů. $group pak agreguje tyto příznaky per platformu a $project převede na procenta. Výsledkem je přehled datové kvality pro každý ze tří zdrojů."

---

### Q26 — Test validačního schématu
> "Dotaz záměrně vloží dva neplatné dokumenty — jeden s source_platform: xbox což není v enum a druhý s prázdným title což porušuje minLength:1. try/catch zachytí chybu kód 121 a zobrazí validační zprávu. Tím se živě ověří že JSON Schema validator skutečně funguje a chrání kolekci před neplatnými daty."

---

### Q27 — explain() index vs COLLSCAN
> "Dotaz porovná plán vykonání se stejným filtrem dvakrát — jednou s indexem a jednou s hint({ $natural: 1 }) který vynutí COLLSCAN bez indexu. explain(executionStats) vrátí totalDocsExamined — počet prohlédnutých dokumentů. S indexem release_year databáze prohlédne výrazně méně dokumentů než při průchodu celé kolekce. Kvantitativně to dokazuje přínos indexu."

---

### Q28 — Partial index
> "Dotaz vytvoří partial index s partialFilterExpression — indexuje pouze Steam hry s cenou nad 30 a ratings nad 1000. Partial index je menší než plný index protože pokrývá jen podmnožinu dokumentů. explain() s hint na tento index pak ověří že dotaz na tuto podmnožinu skutečně index použil místo COLLSCAN. Hodí se pro časté dotazy na specifickou podmnožinu dat."

---

### Q29 — $indexStats
> "Dotaz $indexStats vrátí pro každý index akumulované statistiky od posledního startu — accesses.ops je počet použití. $group seskupí statistiky per index přes všechny shardy a $project přidá příznak is_unused pro indexy s ops rovným nule. Index který se nikdy nepoužívá zbytečně zabírá paměť a zpomaluje zápisy — je kandidátem na smazání."

---

### Q30 — Full-text search
> "Dotaz využívá compound text index vytvořený nad poli title, publisher a developer. $text operátor s řetězcem 'fantasy dragon magic -online' hledá hry obsahující tato slova přičemž minus online je negace — hry s online v názvu jsou vyloučeny. $meta textScore vrátí skóre relevance pro každý dokument. Výsledek kombinuje fulltextovou relevanci se skóre kritiků přes compound $sort."
