# import os 
# import uuid 
from time import sleep
from requests_html import HTML, HTMLSession
from loguru import logger
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError
import re
from time import sleep


class CrawlerML():
    def __init__(self, session, logger) -> None:
        self.session = session
        self.logger = logger

    def _search(self):
        url = "https://imoveis.mercadolivre.com.br/apartamentos/casas-e-apartamentos_NoIndex_True"
        return self.session.get(url=url)
    
    def _get_next_page(self, url):
        return self.session.get(url=url)
    
    def _get_results(self):
        self.logger.info("Buscando resultados")
        for _ in range(10):
            try:
                response = self._search()
                if response.status_code == 200:
                    break
            except:
                sleep(3)
        
        response_content = HTML(session=response, html=response.content)
        count = 1
        while True:
            self.logger.info(f"Página {count}")
            results = response_content.xpath("//a[contains(@class, 'ui-search-result__content ui-search-link')]")
            for result in results:
                yield result    
            next_page_url = response_content.xpath("//a[contains(@class, 'andes-pagination__link shops__pagination-link ui-search-link')]", first=True)
        
            if next_page_url is not None:
                url = next_page_url.attrs["href"]

                for _ in range(10):
                    try:
                        response = self._get_next_page(url)
                        if response.status_code == 200:
                            break
                    except:
                        sleep(3)
                response_content = HTML(session=response, html=response.content)
                sleep(3)
                count += 1
            else:
                break
        self.logger.info("Captura realizada com sucesso")

    def _get_title(self, data):
        try:
            title = data.find("h1", attrs={"class": 'ui-pdp-title'})
            title = title.get_text()
        except:
            pass
        return title

    def _get_price(self, data):
        try:
            price = data.find("span", attrs={"class":"andes-money-amount__fraction"})
            price = price.get_text()
        except:
            pass
        return price

    def _get_local(self, data):
        try:
            local = data.find("div", attrs={"id":"location"})
            local = local.find("p", attrs={"class":"ui-pdp-color--BLACK ui-pdp-size--SMALL ui-pdp-family--REGULAR ui-pdp-media__title"})
            local = local.get_text()
        except:
            pass
        return local

    def _get_description(self, data):
        try:
            description = data.find("p", attrs={"class":"ui-pdp-description__content"})
            description = description.get_text()

            description = re.sub("<br>", "", description)
        except:
            pass
        return description

    def _get_caracteristicas(self, data):
        try:
            caracteristicas = data.find_all("span", attrs={"class":"andes-table__column--value"})

            return {
                "Área Total": caracteristicas[0].get_text(),
                "Área Útil": caracteristicas[1].get_text(),
                "Quartos": caracteristicas[2].get_text(),
                "Banheiros": caracteristicas[3].get_text(),
                "Garagens": caracteristicas[4].get_text()
            }
        except:
            pass

    def _parse_data(self, data):
        page = data.attrs["href"]
        try:
            html = urlopen(page)
        except HTTPError as e:
            logger.error(e)

        try:
            bs = BeautifulSoup(html.read(), "lxml")
        except AttributeError as e:
            logger.error(e)
            return None
        
        return {
            "Título": self._get_title(bs),
            "Preço": self._get_price(bs),
            "Localização": self._get_local(bs),
            "Descrição": self._get_description(bs),
            "Características": self._get_caracteristicas(bs)
        }

    def get_data(self):
        yield from (self._parse_data(result) for result in self._get_results())

if __name__ == "__main__":
    crawler = CrawlerML(
        session = HTMLSession(),
        logger = logger
    )
    logger.add("olx.log")
    for cont, data in enumerate(crawler.get_data()):
        print(data)
