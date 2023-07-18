import scrapy
import json


class BayutSpiderSpider(scrapy.Spider):
    name = "bayut_spider"
    # allowed_domains = ["bayut.com"]
    start_urls = ['https://www.bayut.com/to-rent/property/dubai/']
    max_properties =4000


    def parse(self, response):
        # Extract property listings using XPath
        property_listings = response.xpath('//a[contains(@class, "_287661cb")]/@href').getall()

        # Follow each property url and parse the details
        for url in property_listings:
            yield response.follow(url,callback=self.parse)

        
        # Pagination Link
        next_page = response.css('[title="Next"]::attr(href)').get()
        if next_page is not None:
            next_url = 'https://www.bayut.com' + next_page
            yield response.follow(next_url, callback=self.parse)
 



        for listing in property_listings:
            # Extract property details using XPath
            property_id = listing.xpath('//span[@class="_812aa185"]/text()').getall(),
            purpose = listing.xpath('//span[@class="_812aa185"]/text()').getall(),
            type = listing.xpath('//span[@class="_812aa185"]/text()').getall(),
            added_on = listing.xpath('//span[@class="_812aa185"]/text()').getall(),
            furnishing = listing.xpath('//span[@class="_812aa185"]/text()').getall(),
            price = {'currency':listing.xpath('//span[@class="e63a6bfb"]/text()').get(),
                      'amount':listing.xpath('//span[@class="_105b8a67"]/text()').get()},
            location = listing.xpath('//div[@class="_1f0f1758"]/text()').get()
            bed_bath_size = {"bedrooms":listing.xpath('//span[@class="fc2d1086"]/text()').get(),
                             "bathrooms":listing.xpath('//span[@class="fc2d1086"]/text()').get(),
                             "size": listing.xpath('.//span[@class="fc2d1086"]/span[1]/text()').get(),},
            agent_name = listing.xpath('//a[@class="f730f8e6"]/text()').get(),
            image_url = listing.xpath('//div[contains(@class, "_31cc6dcd")]//img/@src').get(default=''),
            breadcrumbs = listing.xpath('//span[@class="_327a3afc"]/text()').getall(),
            amenities = listing.xpath('//span[@class="_005a682a"]/text()').getall()
            description = listing.xpath('//span[@class="_2a806e1e"]/text()').getall()

            # Clean and structure the data
            cleaned_property_id = self.clean_data(property_id)
            cleaned_purpose = self.clean_data(purpose)
            cleaned_type = self.clean_data(type)
            cleaned_added_on = self.clean_data(added_on)
            cleaned_furnishing = self.clean_data(furnishing)
            cleaned_price = self.clean_data(price)
            cleaned_location = self.clean_data(location)
            cleaned_bed_bath_size = self.clean_data(bed_bath_size)
            cleaned_agent_name = self.clean_data(agent_name)
            cleaned_image_url = self.clean_data(image_url)
            cleaned_breadcrumbs = [self.clean_data(crumb) for crumb in breadcrumbs]
            cleaned_amenities = [self.clean_data(amenity) for amenity in amenities]
            

            # Create a dictionary with the structured data
            data = {
                'Property ID': cleaned_property_id,
                'Purpose': cleaned_purpose,
                'Type': cleaned_type,
                'Added On': cleaned_added_on,
                'Furnishing': cleaned_furnishing,
                'Price': cleaned_price,
                'Location': cleaned_location,
                'Bed Bath Size': cleaned_bed_bath_size,
                'Agent Name': cleaned_agent_name,
                'Image URL': cleaned_image_url,
                'Breadcrumbs': cleaned_breadcrumbs,
                'Amenities': cleaned_amenities,
                
            } 
            yield data

        filename = 'property_data.json'
        with open(filename, 'w') as file:
            json.dump(self.data, file)

    def clean_data(self, data):
        # Perform cleaning operations as required
        # Example: Remove unwanted characters, whitespace, etc.
        cleaned_data = data.strip()
        return cleaned_data
