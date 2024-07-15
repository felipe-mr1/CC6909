from SPARQLWrapper import SPARQLWrapper, JSON

endpoint_url = "https://query.wikibase.imfd.cl/proxy/wdqs/bigdata/namespace/wdq/sparql"

def construct_query(name):
    # Split the input name into parts
    parts = name.split()
    if len(parts) < 2:
        raise ValueError("Please provide both a first name and a last name")

    first_name = parts[0]
    last_name = parts[-1]
    
    # Construct the regex patterns
    patterns = [
        f"{last_name} {first_name[0]}",
        f"{first_name[0]}. {last_name}",
        f"{first_name} {last_name}"
    ]
    
    regex_pattern = "|".join(patterns)
    
    query = f"""
    SELECT ?person ?personLabel WHERE {{
      ?person wdt:P20 wd:Q1;
             rdfs:label ?personLabel.

      # Aplica un filtro REGEX para encontrar coincidencias flexibles
      FILTER (REGEX(?personLabel, "{regex_pattern}", "i")).

      # Esto asegurará que las etiquetas sean devueltas en el idioma preferido
      FILTER (LANG(?personLabel) = "en").
    }}
    """
    return query

def get_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def process_results(results):
    for result in results["results"]["bindings"]:
        print(result["personLabel"]["value"])

def search_person_by_name(name):
    query = construct_query(name)
    results = get_results(endpoint_url, query)
    process_results(results)