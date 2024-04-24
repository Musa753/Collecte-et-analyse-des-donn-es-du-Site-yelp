-------Quel utilisateur  est le plus influent en termes de nombre de fans et d'années d'élite----
     ---0,8 seconde--------

SELECT fu."user_id",
       fu."name",
       fu."fans",
       COUNT(e."year") AS elite_years
FROM FACT_USERS fu
         JOIN ELITE e ON fu."elite_id" = e."elite_id"
GROUP BY fu."user_id", fu."name", fu."fans"
ORDER BY fu."fans" DESC, elite_years DESC;


----utilisateurs avec le plus grands nombre de reviews count
-----3S--------------------
SELECT
     "name",
    SUM("review_count") AS total_reviews
FROM
    FACT_USERS
GROUP BY
    CUBE ("name");
------------------------------------Date yelping la plus frequente--------------
                  ---------------0,6 secondes -----------
SELECT
    t."yelping_since",
    COUNT(*) AS frequency
FROM
    FACT_USERS u
        JOIN
    TIME t ON u."date_id" = t."date_id"
GROUP BY
    t."yelping_since"
ORDER BY
    frequency DESC;
------------------------------------jour de debut  yelping le plus frequent--------------

SELECT
    TO_CHAR(t."yelping_since", 'DAY') AS day_of_week,
    COUNT(*) AS frequency
FROM
    FACT_USERS u
        JOIN
    TIME t ON u."date_id" = t."date_id"
GROUP BY
    t."yelping_since", TO_CHAR(t."yelping_since", 'DAY')
ORDER BY
    frequency DESC


-------utilisateur ayant le plus de fans et de critiques
SELECT
    fu."user_id",
    fu."name",
    fu."fans",
    fu."review_count"
 FROM
    FACT_USERS fu
        JOIN
    ELITE e ON fu."elite_id" = e."elite_id"
GROUP BY ("name","user_id","fans","review_count")
ORDER BY
    fu."fans" DESC,
    fu."review_count" DESC



-------Utilisateurs avec la date de la dernière critique et la plus grande moyenne d'étoiles---------
SELECT
    fu."user_id",
    MAX(r."date") AS latest_review_date,
    fu."average_stars",
    TO_CHAR(r."review_id") AS review_id
FROM
    FACT_USERS fu
        JOIN
    REVIEW r ON fu."review_id" = TO_CHAR(r."review_id")
GROUP BY
    fu."user_id", fu."average_stars",TO_CHAR(r."review_id")
ORDER BY
    fu."average_stars" DESC;


----entreprises les mieux notées -----
---Quelles sont les entreprises les mieux notées ?
----- Les 100 entreprises avec le plus grand nombre de critiques
SELECT DISTINCT  "business_name", "Reviews_stars_count"
FROM FACT_BUSINESS
ORDER BY "Reviews_stars_count" DESC
    FETCH FIRST 100 ROWS ONLY;
----- Les 100 entreprises les mieux notées  par ville
SELECT DISTINCT l."city" , b."business_name", b."Reviews_stars_count"
FROM FACT_BUSINESS b JOIN
    "LOCATION" l ON b."location_id"=l."location_id"
GROUP BY l."city", b."business_name", b."Reviews_stars_count"
ORDER BY "Reviews_stars_count" DESC
    FETCH FIRST 100 ROWS ONLY;
----- Les 100 entreprises les mieux notées  par etat
SELECT DISTINCT l."state",b."business_name", b."Reviews_stars_count"
FROM FACT_BUSINESS b JOIN
     "LOCATION" l ON b."location_id"=l."location_id"
GROUP BY l."state", b."business_name", b."Reviews_stars_count", b."business_id"
ORDER BY "Reviews_stars_count" DESC
    FETCH FIRST 100 ROWS ONLY;
---Les categories les mieux notées
----4 secondes
SELECT c."category",
       SUM(b."Reviews_stars_count") as total_reviews
FROM FACT_BUSINESS  b
         JOIN "CATEGORIES"  c ON b."category_id"=c."category_id"
GROUP BY CUBE (c."category")
ORDER BY total_reviews DESC
    FETCH FIRST 100 ROW ONLY;
-----Quelles sont les entreprises avec  les plus  de critiques utiles? par ville? pays ? Categorie ?
      ---Entreprise les mieux notees en generales
SELECT DISTINCT  "business_name", "Reviews_useful_count"
FROM FACT_BUSINESS
ORDER BY "Reviews_useful_count" DESC
    FETCH FIRST 100 ROWS ONLY;
----- Les 100 entreprises  les plus  de critiques utiles par ville
SELECT DISTINCT l."city" , b."business_name", b."Reviews_stars_count"
FROM FACT_BUSINESS b JOIN
     "LOCATION" l ON b."location_id"=l."location_id"
GROUP BY l."city", b."business_name", b."Reviews_stars_count"
ORDER BY "Reviews_stars_count" DESC
    FETCH FIRST 100 ROWS ONLY;
----- Les 100 entreprises  les plus  de critiques utiles par etat
SELECT DISTINCT l."state",b."business_name", b."Reviews_useful_count"
FROM FACT_BUSINESS b JOIN
     "LOCATION" l ON b."location_id"=l."location_id"
GROUP BY l."state", b."business_name", b."Reviews_useful_count", b."business_id"
ORDER BY "Reviews_useful_count" DESC
    FETCH FIRST 100 ROWS ONLY;
---Les categories  les plus  de critiques utiles
 SELECT c."category",
       SUM(b."Reviews_useful_count") as total_reviews
FROM FACT_BUSINESS  b
         JOIN "CATEGORIES"  c ON b."category_id"=c."category_id"
GROUP BY CUBE (c."category")
ORDER BY total_reviews DESC
    FETCH FIRST 100 ROW ONLY;

------Quelles sont les entreprises les plus visités ?
-----commerce les plus visités ---------------------
    ----------1 Commerces les plus visités en generale -------------------
----- Les 10 commerces les plus visités
------ 21 secondes -------------
SELECT  b."business_name", COUNT(*) AS num_visits
FROM FACT_BUSINESS b
         JOIN CHECKIN c ON b."checkin_id" = c."checkin_id"
 WHERE b."business_name" IS NOT NULL
GROUP BY ROLLUP( b."business_name")
 OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY;
-----Les 10 commerces les plus visités par pays
  ----26 secondes --------------
SELECT l."state", b."business_name", COUNT(*) AS num_visits
FROM FACT_BUSINESS b
         JOIN CHECKIN c ON b."checkin_id" = c."checkin_id"
         JOIN LOCATION l ON b."location_id" = l."location_id"
WHERE b."business_name" IS NOT NULL
GROUP BY ROLLUP(l."state", b."business_name")
ORDER BY l."state", num_visits DESC
OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY;

----- Les 10 commerces les plus visités par ville
     -----29secondes---------------
SELECT l."city", b."business_name", COUNT(*) AS num_visits
FROM FACT_BUSINESS b
         JOIN CHECKIN c ON b."checkin_id" = c."checkin_id"
         JOIN LOCATION l ON b."location_id" = l."location_id"
GROUP BY ROLLUP(l."city", b."business_name")
ORDER BY l."city", num_visits DESC
    FETCH FIRST 10 ROWS ONLY;
----quellles sont les entreprises qui ont reçu le plus de conseils? dans quelle ville ? pays ?
-----Les entreprises ayant reçu le plus conseil en generale---
 ----21 secondes ----
SELECT "business_id",
       "business_name",
       SUM("TIP_compliments_count") AS total_tip_compliments
FROM FACT_BUSINESS
GROUP BY CUBE ("business_id", "business_name")
 ORDER BY total_tip_compliments DESC
    FETCH FIRST 100 ROW ONLY;
---Les categories ayant reçu le plus de conseils
SELECT c."category",
        SUM("TIP_compliments_count") AS total_tip_compliments
FROM FACT_BUSINESS  b
         JOIN "CATEGORIES"  c ON b."category_id"=c."category_id"
GROUP BY CUBE (c."category")
ORDER BY total_tip_compliments DESC
    FETCH FIRST 100 ROW ONLY;
----Quel jour de la semaine reçoit le plus de visites ?
----- Pour afficher le jour avec le plus de check-ins,
  ----4S------------
SELECT TO_CHAR(c."date", 'Day') AS day_of_week,
       COUNT(DISTINCT b."checkin_id") AS total_checkins
FROM CHECKIN c
         JOIN FACT_BUSINESS b ON c."checkin_id" = b."checkin_id"
GROUP BY TO_CHAR(c."date", 'Day')
ORDER BY COUNT(DISTINCT b."checkin_id") DESC;
----- Pour afficher le jour avec le plus de check-ins par ville
SELECT l."city", TO_CHAR(c."date", 'Day') AS day_of_week,
       COUNT(DISTINCT b."checkin_id") AS total_checkins
FROM CHECKIN c
         JOIN FACT_BUSINESS b ON c."checkin_id" = b."checkin_id"
         JOIN  LOCATION l ON l."location_id"=b."location_id"
GROUP BY TO_CHAR(c."date", 'Day'),l."city"
ORDER BY COUNT(DISTINCT b."checkin_id") DESC;