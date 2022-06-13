# IMDB Titles Scraper By Company

This actor allows you to scrape titles from IMDB that created or disturbed by a specific company. All you need to do in 
order to start the scraper is to get the company id which is the main input "for now" in the scraper.

## Input configuration

The actor has the following input options:

- **Company ID** - Paste the company id you get from IMDB into this field.
- **Testing** - Optionally, select yes to scrape only the first 50 movies.
- **Proxy** - Optionally, select a proxy to be used by the actor,
  in order to avoid IP address-based blocking by the target website.
  The actor automatically executes all the Scrapy's HTTP(S) requests through the proxy.

## How to get the Company ID?

It might sound complicated, but it is too easy...

1. First, from the main IMDB search bar, select Companies...

   ![](https://raw.githubusercontent.com/lxth0rz/imgs_proc/main/step_1.png)


2. Then, type the name of the company...

   ![](https://github.com/lxth0rz/imgs_proc/blob/main/step_2.png?raw=true)


3. Click the search icon, the pick the company you would like to scrape their title from the results...
 
   ![](https://github.com/lxth0rz/imgs_proc/blob/main/step_3.png?raw=true)


4. Finally, get the company id from the URL. The id in this example is: **co0047120**.

   ![](https://github.com/lxth0rz/imgs_proc/blob/main/step_4.png?raw=true/step_4.png)

## Results:

Each record represent a title. The following fields is the current data you will get from the scraper...

```json
{
    "id": "tt4574334",
    "title": "Stranger Things",
    "year": "2016–",
    "certificate": "15",
    "runtime": "51 min",
    "genre": "Drama, Fantasy, Horror",
    "rating": "8.7",
    "plot": "When a young boy disappears, his mother, a police chief and his friends must confront terrifying supernatural forces in order to get him back.",
    "stars": "Millie Bobby Brown| Finn Wolfhard| Winona Ryder| David Harbour",
    "votes": "1,031,928",
    "url": "https://www.imdb.com/title/tt4574334/",
    "poster_url": "https://m.media-amazon.com/images/M/MV5BODZlYjQ4NzYtZTg1MC00NGY4LTg4NjQtNGE3ZjRkMjk3YjMyXkEyXkFqcGdeQXVyMTkxNjUyNQ@@._V1_UY98_CR5,0,67,98_AL_.jpg",
    "big_poster_url": "https://m.media-amazon.com/images/M/MV5BODZlYjQ4NzYtZTg1MC00NGY4LTg4NjQtNGE3ZjRkMjk3YjMyXkEyXkFqcGdeQXVyMTkxNjUyNQ@@._V1_SY1000_CR0,0,674,1000_AL_.jpg"
  }
```

And That's it for now!

If you have any problem or anything does not work,
please file an [issue on Apify](https://console.apify.com/actors/LWCaRh1QoRdiI8siz#/issues).
