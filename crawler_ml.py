from time import sleep
from requests_html import HTMLSession
from loguru import logger
import re
from time import sleep
from client import Client

class CrawlerML():
    def __init__(self, session, logger) -> None:
        self.session = session
        self.logger = logger

    def _search(self):
        url = "https://imoveis.mercadolivre.com.br/apartamentos/casas-e-apartamentos_NoIndex_True"
        return self.session.get(url=url)
    
    def _get_next_page(self, url):
        return self.session.get(url=url)

    def _get_page_content(self, url):
        for _ in range(10):
            response = self.session.get(url=url)
            self.logger.info(response.status_code)
            if response.status_code == 200:
                return response
    
    def _get_results(self):
        self.logger.info("Buscando resultados")
        for _ in range(10):
            try:
                response = self._search()
                if response.status_code == 200:
                    break
            except:
                sleep(3)

        pages = response.html.xpath("//li[@class='andes-pagination__page-count']", first=True)
        pages = self._perform_regex(r'[0-9]+', pages.text)
        pages = int(pages)
        self.logger.info(f"Foram encontradas {pages} páginas")

        for page_n in range(1, pages+1):
            self.logger.info(f"Página {page_n} de {pages}")
            results = response.html.xpath("//a[@class='ui-search-result__content ui-search-link']")
            for result in results:
                page_content = result.attrs["href"]
                page_content = self._get_page_content(page_content)
                yield page_content
                
            if page_n>1:
                next_page_url = response.html.xpath("//a[@class='andes-pagination__link shops__pagination-link ui-search-link']", first=True)
                
                if next_page_url is not None:
                    url = next_page_url.attrs["href"]
                    for _ in range(10):
                        try:
                            response = self._get_next_page(url)
                            if response.status_code == 200:
                                break
                        except:
                            sleep(3)
                        sleep(3)
                else:
                    break
    
    def _perform_regex(self, pattern, text):
        self.logger.info("Performing " + pattern)
        m = re.search(pattern, text)
        try:
            match = m.group(0)
        except AttributeError:
            self.logger.warning("Regex not found")
            return None
        self.logger.info("Found " + match)
        return match
            
    def _get_title(self, data):
        title = data.html.xpath("//h1[@class='ui-pdp-title']", first=True)
        
        if title is not None:
            return title.text
        else:
            return None

    def _get_price(self, data):
        price = data.html.xpath("//span[@class='andes-money-amount__fraction']", first=True)

        if price is not None:
            return price.text
        else:
            return None

    def _get_local(self, data):
        local = data.html.xpath("//p[@class='ui-pdp-color--BLACK ui-pdp-size--SMALL ui-pdp-family--REGULAR ui-pdp-media__title']", first=True)
        
        if local is not None:
            return local.text
        else:
            return None

    def _get_description(self, data):
        description = data.html.xpath("//p[@class='ui-pdp-description__content']", first=True)

        if description is not None:
            description = re.sub("<br>", "", description.text)
            return description
        else:
            return None

    def _get_caracteristicas(self, data):
        caracteristicas = data.html.xpath("//span[@class='andes-table__column--value']")

        try: 
            data = {
                "Área Total": caracteristicas[0].text if caracteristicas[0].text is not None else None,
                "Área Útil": caracteristicas[1].text if caracteristicas[1].text is not None else None,
                "Quartos": caracteristicas[2].text if caracteristicas[2].text is not None else None,
                "Banheiros": caracteristicas[3].text if caracteristicas[3].text is not None else None
            }
        except:
            data = None

        if data is not None:
            return data
        else:
            return None

    def _parse_data(self, data):
        
        data = {
            "Título": self._get_title(data),
            "Preço": self._get_price(data),
            "Localização": self._get_local(data),
            "Descrição": self._get_description(data),
            "Características": self._get_caracteristicas(data)
        }

        if data is not None:
            return data
        else:
            return None

    def get_data(self):
        yield from (self._parse_data(data) for data in self._get_results())

if __name__ == "__main__":
    crawler = CrawlerML(
        session = HTMLSession(),
        logger = logger
    )
    client = Client()
    logger.add("olx.log")
    for cont, data in enumerate(crawler.get_data()):
        client.upload_data(payload=data)
