import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

MAX_THREADS = 10
CSV_FILE = 'movies.csv'


def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0.2, 0.6))

        response = requests.get(movie_link, headers=HEADERS, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Título
        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else None

        # Data de lançamento
        date_tag = soup.find('a', href=lambda x: x and 'releaseinfo' in x)
        date = date_tag.get_text(strip=True) if date_tag else None

        # Nota
        rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        rating = rating_tag.get_text(strip=True) if rating_tag else None

        # Sinopse
        plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        plot_text = plot_tag.get_text(strip=True) if plot_tag else None

        if not any([title, date, rating, plot_text]):
            print('Falha ao extrair:', movie_link)
            return

        print(title, date, rating)

        with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([title, date, rating, plot_text])

    except Exception as e:
        print('Erro em:', movie_link, '|', e)


def extract_movies(soup):
    container = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
    if not container:
        print('Não foi possível achar a lista de filmes.')
        return

    movies_table = container.find('ul')
    movies = movies_table.find_all('li')

    movie_links = []
    for movie in movies:
        a_tag = movie.find('a')
        if a_tag and a_tag.get('href'):
            movie_links.append('https://www.imdb.com' + a_tag['href'])

    threads = min(MAX_THREADS, len(movie_links))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


def init_csv():
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Release Date', 'Rating', 'Plot'])


def main():
    start_time = time.time()

    init_csv()

    url = 'https://www.imdb.com/chart/moviemeter/'
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    print('Tempo total:', round(end_time - start_time, 2), 'segundos')


if __name__ == '__main__':
    main()
