ARG PY_VERSION=3.10.6
FROM python:${PY_VERSION}-buster

WORKDIR /src

COPY csgo_scraper/app.py csgo_scraper/app.py
COPY csgo_scraper/scraper.py csgo_scraper/scraper.py
COPY csgo_scraper/webhook.py csgo_scraper/webhook.py
COPY csgo_scraper/__init__.py csgo_scraper/__init__.py
COPY requirements.txt requirements.txt
COPY setup.py setup.py

RUN pip install --upgrade pip
RUN pip install .

CMD python csgo_scraper/app.py
