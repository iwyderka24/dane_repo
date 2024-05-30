# NIE EDYTOWAĆ *****************************************************************

library(DBI)
library(RPostgres)
library(testthat)

con <- dbConnect(Postgres(), dbname = dsn_database, host=dsn_hostname, port=dsn_port, user=dsn_uid, password=dsn_pwd) 
# ******************************************************************************

film_in_category<- function(category_id)
{
  # Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
  # Przykład wynikowej tabeli:
  # |   |title          |language    |category|
  # |0	|Amadeus Holy	|English	|Action|
  # 
  # Tabela wynikowa ma być posortowana po tylule filmu i języku.
  # 
  # Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL
  # 
  # Parameters:
  # category_id (integer): wartość id kategorii dla którego wykonujemy zapytanie
  # 
  # Returns:
  # DataFrame: DataFrame zawierający wyniki zapytania
  #
  if (!is.integer(category_id)) {
    return(NULL)
  } else {
    query <- paste(
      "SELECT 
    f.title AS title,
    l.name AS language,
    c.name AS category
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    INNER JOIN language AS l ON f.language_id = l.language_id
    WHERE c.category_id = ",toString(category_id),
    " ORDER BY
    title ASC,
    language ASC", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
}


number_films_in_category <- function(category_id){
  #   Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
  #     Przykład wynikowej tabeli:
  #     |   |category   |count|
  #     |0	|Action 	|64	  | 
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     category_id (integer): wartość id kategorii dla którego wykonujemy zapytanie
  #     
  #     Returns:
  #     DataFrame: DataFrame zawierający wyniki zapytania
  
  if (!is.integer(category_id)) {
    return(NULL)
  } else {
    query <- paste(
      "SELECT
    c.name AS category, 
    COUNT(fc.film_id) AS count
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    WHERE fc.category_id = ",toString(category_id), 
    "\n GROUP BY name", sep ="")
    df <- dbGetQuery(con, query)
    return(df)
  }
}

number_film_by_length <- function(min_length, max_length){
  #   Funkcja zwracająca wynik zapytania do bazy o ilość filmów dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
  #     Przykład wynikowej tabeli:
  #     |   |length     |count|
  #     |0	|46 	    |64	  | 
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     min_length (int,double): wartość minimalnej długości filmu
  #     max_length (int,double): wartość maksymalnej długości filmu
  #     
  #     Returns:
  #     pd.DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.numeric(min_length) || !is.numeric(max_length) || min_length > max_length) {
    return(NULL)
  } else {
    query <- paste(
    "SELECT 
    f.length AS length,
    COUNT(film_id) AS count
    FROM film AS f
    WHERE length
    BETWEEN ", toString(min_length)," AND ",toString(max_length),
    "\n GROUP BY length", sep = "")
    dc <- dbGetQuery(con, query)
  }
}


client_from_city<- function(city){
  #   Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
  #     Przykład wynikowej tabeli:
  #     |   |city	    |first_name	|last_name
  #     |0	|Athenai	|Linda	    |Williams
  #     
  #     Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     city (character): nazwa miaste dla którego mamy sporządzić listę klientów
  #     
  #     Returns:
  #     DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.character(city)) {
    return(NULL)
  } else {
    query <- query <- paste(
    "SELECT 
    ci.city AS city,
    c.first_name AS first_name, 
    c.last_name AS last_name
    FROM customer AS c
    INNER JOIN address AS a ON c.address_id = a.address_id
    INNER JOIN city AS ci ON a.city_id = ci.city_id
    WHERE city = '", toString(city),
    "'\n ORDER BY last_name, first_name",
    sep = "")
    dc <- dbGetQuery(con, query)
  }
}

avg_amount_by_length<-function(length){
  #   Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
  #     Przykład wynikowej tabeli:
  #     |   |length |avg
  #     |0	|48	    |4.295389
  #     
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     length (integer,double): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
  #     
  #     Returns:
  #     DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.numeric(length)) {
    return(NULL)
  } else {
    query <- paste(
    "SELECT 
    f.length AS length,
    AVG(p.amount) AS avg
    FROM film AS f
    INNER JOIN inventory AS i ON f.film_id = i.film_id
    INNER JOIN rental AS r ON i.inventory_id = r.inventory_id
    INNER JOIN payment AS p ON r.rental_id = p.rental_id
    WHERE length = ", toString(length),
    "\n GROUP BY length"
      , sep = "")
    dc <- dbGetQuery(con, query)
  }
}


client_by_sum_length<-function(sum_min){
  #   Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
  #     Przykład wynikowej tabeli:
  #     |   |first_name |last_name  |sum
  #     |0  |Brian	    |Wyman  	|1265
  #     
  #     Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     sum_min (integer,double): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
  #     
  #     Returns:
  #     DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.numeric(sum_min)) {
    return(NULL)
  } else {
    query <- paste(
    "SELECT 
    c.first_name AS first_name,
    c.last_name AS last_name,
    SUM(f.length) AS sum
    FROM customer AS c
    INNER JOIN rental AS r ON c.customer_id = r.customer_id
    INNER JOIN inventory AS i ON r.inventory_id = i.inventory_id
    INNER JOIN film AS f ON i.film_id = f.film_id
    GROUP BY last_name, first_name
    HAVING SUM(f.length) > ", toString(sum_min),
    "\n ORDER BY sum, last_name, first_name"
    , sep = "")
    dc <- dbGetQuery(con, query)
  }
}


category_statistic_length<-function(name){
  #   Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
  #     Przykład wynikowej tabeli:
  #     |   |category   |avg    |sum    |min    |max
  #     |0	|Action 	|111.60 |7143   |47 	|185
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  #     Parameters:
  #     name (character): Nazwa kategorii dla której ma zostać wypisana statystyka
  #     
  #     Returns:
  #     DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.character(name)) {
    return(NULL)
  } else {
    query <- paste(
    "SELECT
    c.name AS category,
    AVG(f.length) AS avg,
    SUM(f.length) AS sum,
    MIN(f.length) AS min,
    MAX(f.length) AS max
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    WHERE name = '", toString(name),
    "'\n GROUP BY name"
    , sep = "")
    dc <- dbGetQuery(con, query)
  }
}

# NIE EDYTOWAĆ *****************************************************************
test_dir('tests/testthat')
# ******************************************************************************