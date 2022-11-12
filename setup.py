from setuptools import find_packages , setup

with open("requirements.txt" , encoding = "utf-8" , mode = "r+") as f:
    content = f.readlines()

content = [req.strip() for req in content]

setup(version = "0.0.1",
        name = "csgo_scraper",
        author = "Channi",
        description = "CSGO-match-scraper",
        packages = find_packages(),
        install_requires = content
        )
