import requests
import os
import json
from discord import SyncWebhook , Embed , Color
from datetime import datetime
from pytz import timezone
from dataclasses import dataclass , field

@dataclass
class DiscordClient:

    title : str
    webhook_url : str = os.getenv("WEBHOOK_URL")
    url: str = os.getenv("IMAGE_URL")
    redirect_url : str = os.getenv("REDIRECT_URL")
    location : str = os.getenv("LOCATION")

    def send_webhook(self , description : str , data_length : int = None , unique_reports_number : int = None) -> None:
        webhook = SyncWebhook.from_url( self.webhook_url
                            ) # Initializing webhook
        embed = Embed(title=self.title,
                      description=description,
                      url=self.redirect_url,
                      timestamp=datetime.now(timezone(self.location)))

        embed.set_image(
            url = self.url
        )

        if data_length:
            embed.add_field(name = "Scraped Reports", value = data_length)
            embed.add_field(name="Unique Report IDs", value=unique_reports_number)
            embed.add_field(name = "Date" , value = datetime.now(timezone(self.location)).strftime("%y-%m-%d %H:%M:%S"))
        else:
            embed.add_field(name="Fatal Error" , value = "Failed to scrape the page")
        webhook.send(embed = embed ,
                    content= "CSGO: WebScraping Hourly Report")

if __name__ == "__main__":

    ##test
    message = "Welcome!"
    DiscordClient(title="csgo").send_webhook(message)
