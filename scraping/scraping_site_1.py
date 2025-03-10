from .base_scraper import BaseScraper
import pandas as pd

class Site1Scraper(BaseScraper):
    def scrape(self, url):
        soup = self.fetch_page(url)
        data = []
        # Lógica específica para pagina holandesa
        sections = soup.find_all('article', class_='container')
        for section in sections:
            headers = section.find_all('h2', class_='h3 mt-4')
            for header in headers:
                section_name = header.get_text(strip=True)
                list_group = header.find_next('div', class_='list-group striped-rows')
                if not list_group:
                    continue

                items = list_group.find_all('div', class_='list-group-item')
                for item in items:
                    key = item.find('div', class_='col-sm-6 one-line text-sm-bold')
                    value = item.find('div', class_='col-sm-6 one-line')

                    if key and value:
                        key = key.get_text(strip=True)
                        value = value.get_text(strip=True)
                        combined_key = f"{section_name} - {key}"
                        data.append((combined_key, value))

        return pd.DataFrame(data, columns=["Key", "Value"])
