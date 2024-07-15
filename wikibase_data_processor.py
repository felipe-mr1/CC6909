import pandas as pd
from wikidataintegrator import wdi_core, wdi_login
from datetime import datetime
from urllib.parse import urlparse
import time

class WikibaseHandler:
    # Required fields for the data
    REQUIRED_FIELDS = [
        'Fecha Publicación', 'Categoria Publicación', 'Titulo', 'Autor(es)', 'Fuente', 'Volumen', 
        'Numero', 'Pagina Inicial', 'ISSN', 'DOI', 'Cuartil', 'Cuartil criterio IMFD',
        'Red Formal en la que Participa', 'Líneas de Investigación', 
        'N° investigadores asociados del centro', 'N° investigadores del centro otra categoria', 'N° estudiantes'
    ]
    # Item types
    RESEARCH_ARTICLE = 1
    AUTHOR = 2

    def __init__(self, user, password, api_url):
        self.login = wdi_login.WDLogin(user=user, pwd=password, mediawiki_api_url=api_url)
        self.api_url = api_url
        self.cache = {}

    """
    Function to get or create a Wikibase item based on the title.
    Parameters:
        title (str): The title of the item.
        item_type (int): The type of the item (RESEARCH_ARTICLE or AUTHOR).
    Returns:
        str: The Wikibase item ID.
    """
    def get_or_create_item(self, title, item_type=0):
        if title in self.cache:
            print(f"Item: {title} already exists in the cache.")
            return self.cache[title]

        item = wdi_core.WDItemEngine.get_wd_search_results(title, mediawiki_api_url=self.api_url)
        if item:
            print(f"Item: {title} already exists.")
            return item[0]

        print(f"Item: {title} does not exist. Creating a new item...")
        new_item = wdi_core.WDItemEngine(data=[], mediawiki_api_url=self.api_url)
        new_item.set_label(title, lang="en")
        new_item.write(self.login)

        if item_type == self.RESEARCH_ARTICLE:
            self._add_research_article_statements(new_item)
        elif item_type == self.AUTHOR:
            self._add_author_statements(new_item)

        self.cache[title] = new_item.wd_item_id
        return new_item.wd_item_id

    def _add_research_article_statements(self, item):
        statements = [wdi_core.WDItemID("Q2", prop_nr="20")]  # Instance of: research article
        article = wdi_core.WDItemEngine(wd_item_id=item.wd_item_id, data=statements, mediawiki_api_url=self.api_url)
        article.write(self.login)

    def _add_author_statements(self, item):
        statements = [wdi_core.WDItemID("Q1", prop_nr="20")]  # Instance of: human
        author = wdi_core.WDItemEngine(wd_item_id=item.wd_item_id, data=statements, mediawiki_api_url=self.api_url)
        author.write(self.login)

class DataProcessor:
    def __init__(self, file_path, wikibase_handler):
        self.data = pd.read_excel(file_path)
        self.wikibase_handler = wikibase_handler

    """
    Function to process the data from the Excel file.
    """
    def process_data(self):
        print(f"This script expects the following fields: {', '.join(WikibaseHandler.REQUIRED_FIELDS)}")
        print("Consider changing the column names in the file or the script if they are different.")
        response = input("Do you want to continue? [yes/no]: ").strip().lower()
        if response != 'yes':
            print("Exiting the script.")
            return
        start_time = time.time()
        
        for index, row in self.data.iterrows():
            if row.isnull().all():
                break
            self._process_row(row)

        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds.")

    def _process_row(self, row):
        title = row['Titulo']
        item_id = self.wikibase_handler.get_or_create_item(title, WikibaseHandler.RESEARCH_ARTICLE)
        publication_date = self._format_date(row['Fecha Publicación'])
        doi_identifier = self._extract_doi(row['DOI']) if not pd.isnull(row['DOI']) else None
        related_url = self._process_url(row['DOI']) if not pd.isnull(row['DOI']) else None

        properties = self._gather_properties(row, publication_date, doi_identifier, related_url)
        item_statements = self._create_statements(properties, row)

        existing_item = wdi_core.WDItemEngine(wd_item_id=item_id, data=item_statements, mediawiki_api_url=self.wikibase_handler.api_url)
        existing_item.write(self.wikibase_handler.login)
        print(f"Item: {title} updated successfully.")

    def _format_date(self, date):
        if isinstance(date, datetime):
            date = date.strftime("%d-%m-%Y")
        return datetime.strptime(date, "%d-%m-%Y").strftime("+%Y-%m-%dT%H:%M:%SZ")

    def _extract_doi(self, doi):
        if "doi.org" in doi:
            if not doi.startswith("http"):
                doi = "https://" + doi
            return urlparse(doi).path.lstrip('/')
        return None
        
    def _process_url(self, url):
        if "doi.org" not in url:
            return url
        return None

    def _gather_properties(self, row, publication_date, doi_identifier, related_url):
        ############################################################
        #### Define properties and their corresponding values ######
        ##  https://wikibase.imfd.cl/wiki/Special:ListProperties  ##
        # Change row names if they are different in the Excel file #
        # P2 - Fecha Publicación     | P11 - DOI                   #
        # P3 - Categoria Publicación | P12 - Cuartil               #
        # P5 - Fuente                | P13 - Cuartil criterio IMFD #
        # P6 - Volumen               | P14 - Red Formal            #
        # P7 - Numero                | P16 - N° investigadores     #
        # P8 - Titulo                | P17 - N° investigadores     #
        # P9 - Pagina Inicial        | P18 - N° estudiantes        #
        # P10 - ISSN                                               #
        ############################################################
        return {
            'P2': publication_date,
            'P3': self.wikibase_handler.get_or_create_item(row['Categoria Publicación']) if not pd.isnull(row['Categoria Publicación']) else None,
            'P5': self.wikibase_handler.get_or_create_item(row['Fuente']) if not pd.isnull(row['Fuente']) else None,
            'P6': str(row['Volumen']) if not pd.isnull(row['Volumen']) else None,
            'P7': str(row['Numero']) if not pd.isnull(row['Numero']) and isinstance(row['Numero'], int) else None,
            'P8': row['Titulo'],
            'P9': str(row['Pagina Inicial']) if not pd.isnull(row['Pagina Inicial']) else None,
            'P10': str(row['ISSN']) if not pd.isnull(row['ISSN']) else None,
            'P11': doi_identifier,
            'P12': row['Cuartil'] if not pd.isnull(row['Cuartil']) else None,
            'P13': row['Cuartil criterio IMFD'] if not pd.isnull(row['Cuartil criterio IMFD']) else None,
            'P14': self.wikibase_handler.get_or_create_item(row['Red Formal en la que Participa']) if not pd.isnull(row['Red Formal en la que Participa']) else None,
            'P16': row['N° investigadores asociados del centro'] if not pd.isnull(row['N° investigadores asociados del centro']) else None,
            'P17': row['N° investigadores del centro otra categoria'] if not pd.isnull(row['N° investigadores del centro otra categoria']) else None,
            'P18': row['N° estudiantes'] if not pd.isnull(row['N° estudiantes']) else None,
            'P24': related_url
        }

    def _create_statements(self, properties, row):
        item_statements = []

        authors = row['Autor(es)'].split(',')
        author_statements = [wdi_core.WDItemID(self.wikibase_handler.get_or_create_item(author.strip(), WikibaseHandler.AUTHOR), prop_nr="P4") for author in authors]
        item_statements.extend(author_statements)

        research_lines = row['Líneas de Investigación'].split(';')
        research_line_statements = [wdi_core.WDItemID(self.wikibase_handler.get_or_create_item(research_line.strip()), prop_nr="P15") for research_line in research_lines]
        item_statements.extend(research_line_statements)

        if properties['P2']:
            item_statements.append(wdi_core.WDTime(properties['P2'], prop_nr="P2"))
        if properties['P3']:
            item_statements.append(wdi_core.WDItemID(properties['P3'], prop_nr="P3"))
        if properties['P5']:
            item_statements.append(wdi_core.WDItemID(properties['P5'], prop_nr="P5"))
        if properties['P6']:
            item_statements.append(wdi_core.WDString(properties['P6'], prop_nr="P6"))
        if properties['P7']:
            item_statements.append(wdi_core.WDString(properties['P7'], prop_nr="P7"))
        if properties['P8']:
            item_statements.append(wdi_core.WDString(properties['P8'], prop_nr="P8"))
        if properties['P9']:
            item_statements.append(wdi_core.WDString(properties['P9'], prop_nr="P9"))
        if properties['P10']:
            item_statements.append(wdi_core.WDExternalID(properties['P10'], prop_nr="P10"))
        if properties['P11']:
            item_statements.append(wdi_core.WDExternalID(properties['P11'], prop_nr="P11"))
        if properties['P12']:
            item_statements.append(wdi_core.WDString(properties['P12'], prop_nr="P12"))
        if properties['P13']:
            item_statements.append(wdi_core.WDString(properties['P13'], prop_nr="P13"))
        if properties['P14']:
            item_statements.append(wdi_core.WDItemID(properties['P14'], prop_nr="P14"))
        if properties['P16']:
            item_statements.append(wdi_core.WDQuantity(properties['P16'], prop_nr="P16"))
        if properties['P17']:
            item_statements.append(wdi_core.WDQuantity(properties['P17'], prop_nr="P17"))
        if properties['P18']:
            item_statements.append(wdi_core.WDQuantity(properties['P18'], prop_nr="P18"))
        if properties['P24']:
            item_statements.append(wdi_core.WDUrl(properties['P24'], prop_nr="P24"))

        return item_statements

def main():
    user = '****'
    password = '****'
    api_url = 'https://wikibase.imfd.cl/w/api.php'
    file_path = "file.xlsx"

    wikibase_handler = WikibaseHandler(user, password, api_url)
    data_processor = DataProcessor(file_path, wikibase_handler)
    data_processor.process_data()

if __name__ == "__main__":
    main()