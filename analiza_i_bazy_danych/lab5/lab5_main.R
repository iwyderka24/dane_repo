# NIE EDYTOWAĆ *****************************************************************

library(DBI)
library(RPostgres)
library(testthat)

con <- dbConnect(Postgres(), dbname = dsn_database, host=dsn_hostname, port=dsn_port, user=dsn_uid, password=dsn_pwd) 
# ******************************************************************************

film_in_category <- function(category)
{
  # Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
  #     - id: jeżeli categry jest integer
  #     - name: jeżeli category jest character, dokładnie taki jak podana wartość
  # Przykład wynikowej tabeli:
  # |   |title          |languge    |category|
  # |0	|Amadeus Holy	|English	|Action|
  # 
  # Tabela wynikowa ma być posortowana po tylule filmu i języku.
  # 
  # Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  # 
  # Parameters:
  # category (integer,character): wartość kategorii po id (jeżeli typ integer) lub nazwie (jeżeli typ character)  dla którego wykonujemy zapytanie
  # 
  # Returns:
  # DataFrame: DataFrame zawierający wyniki zapytania
  
  if (!is.numeric(category) && !is.character(category)) {
    return(NULL)
  } else if (is.numeric(category) && category %% 1 == 0) {
    query <- paste(
    "SELECT 
    f.title AS title,
    l.name AS languge,
    c.name AS category
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    INNER JOIN language AS l ON f.language_id = l.language_id
    WHERE c.category_id = ",toString(category),
      " ORDER BY
    title ASC,
    languge ASC", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
  else if (is.character(category)) {
    query <- paste(
      "SELECT 
    f.title AS title,
    l.name AS languge,
    c.name AS category
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    INNER JOIN language AS l ON f.language_id = l.language_id
    WHERE c.name = '",toString(category),
      "' ORDER BY
    title ASC,
    languge ASC", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
  else {
    return(NULL)
  }
}


film_in_category_case_insensitive <- function(category)
{
  #  Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
  #     - id: jeżeli categry jest integer
  #     - name: jeżeli category jest character
  #  Przykład wynikowej tabeli:
  #     |   |title          |languge    |category|
  #     |0	|Amadeus Holy	|English	|Action|
  #     
  #   Tabela wynikowa ma być posortowana po tylule filmu i języku.
  #     
  #     Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  
  #   Parameters:
  #   category (integer,str): wartość kategorii po id (jeżeli typ integer) lub nazwie (jeżeli typ character)  dla którego wykonujemy zapytanie
  #
  #   Returns:
  #   DataFrame: DataFrame zawierający wyniki zapytania
  if (!is.numeric(category) && !is.character(category)) {
    return(NULL)
  } else if (is.numeric(category) && category %% 1 == 0 && typeof(category) == "integer") {
    query <- paste(
      "SELECT 
    f.title AS title,
    l.name AS languge,
    c.name AS category
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    INNER JOIN language AS l ON f.language_id = l.language_id
    WHERE c.category_id = ",toString(category),
      " ORDER BY
    title ASC,
    languge ASC", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
  else if (is.character(category)) {
    query <- paste(
      "SELECT 
    f.title AS title,
    l.name AS languge,
    c.name AS category
    FROM category AS c
    INNER JOIN film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN film AS f ON fc.film_id = f.film_id
    INNER JOIN language AS l ON f.language_id = l.language_id
    WHERE c.name ILIKE '",toString(category),
      "' ORDER BY
    title ASC,
    languge ASC", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
  else {
    return(NULL)
  }
}

film_cast <- function(title)
{
  # Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
  # Przykład wynikowej tabeli:
  #     |   |first_name |last_name  |
  #     |0	|Greg       |Chaplin    | 
  #     
  # Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
  # Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  # Parameters:
  # title (character): wartość id kategorii dla którego wykonujemy zapytanie
  #     
  # Returns:
  # DataFrame: DataFrame zawierający wyniki zapytania
  
  if (typeof(title) != "character") {
    return(NULL)
  }
  else {
    query <- paste(
    "SELECT 
    a.first_name AS first_name,
    a.last_name AS last_name
    FROM actor AS a
    INNER JOIN film_actor AS fa ON a.actor_id = fa.actor_id
    INNER JOIN film AS f ON fa.film_id = f.film_id
    WHERE f.title = '",toString(title),
    "' ORDER BY last_name, first_name ", sep= "")
    df <- dbGetQuery(con, query)
    return(df)
  }
  
}


film_title_case_insensitive <- function(words)
{
  # Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
  # Przykład wynikowej tabeli:
  #     |   |title              |
  #     |0	|Crystal Breaking 	| 
  #     
  # Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
  # 
  # Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość NULL.
  #         
  # Parameters:
  # words(list[character]): wartość minimalnej długości filmu
  #     
  # Returns:
  # DataFrame: DataFrame zawierający wyniki zapytania
  # 
  if (!is.character(words)) {
    return(NULL)
  }
  else {
    query <- paste(
      "SELECT 
    f.title AS title
    FROM film AS f
    WHERE ", paste("title ILIKE '", words, " %'", sep = "", collapse = " OR "),
    " OR ", paste("title ILIKE '% ", words, "'", sep = "", collapse = " OR "),
    " OR ", paste("title ILIKE '% ", words, " %'", sep = "", collapse = " OR "),
    "ORDER BY title",
      sep = "")
    df <- dbGetQuery(con, query)
    return(df)
  }
}

# NIE EDYTOWAĆ *****************************************************************
test_dir('tests/testthat')
# ******************************************************************************