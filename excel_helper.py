import json
import openpyxl

def write_data_to_file(data, country, countries_map):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f'4K-{country}'

    headers = [
        'Country', 'Title', 'Title Sub Heading', 'Production Company', 'Production Year', 'Film Time', 'Rating', 'Disc Release Date', 'Video Codec', 'Video Encoding', 'Video Resolution', 'Video Aspect Ratio', 'Original Aspect Ratio', 'Audio', 'Subtitles', 'Discs', 'Packaging', 'Playback', 'Genres', 'ISBN', 'EAN', 'UPC', 'SKU(Amazon)', 'eBay EPID', 'New Price', 'Used Price', '3rd Party Used Current', '3rd Party Used Average', 'Amazon Price Current', 'Amazon Price Average', 'Description', 'Director', 'Writer', 'Starring', 'Producers', 'Blu-Ray.com URL', 'SALIENT ID', 'Front Photo', 'Back Photo', 'Slip Photo', 'Slip Back Photo', 'Overview Photo', 'Screenshots'
    ]

    ws.append(headers)
    
    for dvd in data:
        if dvd:
            row = [
                countries_map[country],
                dvd.get('title', ''),
                dvd.get('subheading_title', ''),
                dvd.get('production', ''),
                dvd.get('releaseYear', ''),
                dvd.get('runtime', ''),
                dvd.get('age_rating', ''),
                dvd.get('release_date', ''),
                dvd.get('codec', ''),
                dvd.get('encoding', ''),
                dvd.get('resolution', ''),
                dvd.get('aspect_ratio', ''),
                dvd.get('original_aspect_ratio', ''),
                dvd.get('audio', ''),
                dvd.get('subtitles', ''),
                ','.join(dvd.get('discs', [])),
                ','.join(dvd.get('packaging', [])),
                ','.join(dvd.get('playback', [])),
                ','.join(dvd.get('genres', [])),
                dvd.get('isbn', ''),
                dvd.get('ean', ''),
                dvd.get('upc', ''),
                dvd.get('sku', ''),
                dvd.get('epid', ''),
                dvd.get('new_price', '') if dvd.get('new_price', '') != 'New' else '', 
                dvd.get('used_price', '') if dvd.get('used_price', '') != 'Used' else '',
                dvd.get('third_used_current_price', ''),
                dvd.get('third_used_average_price', ''),
                dvd.get('amazon_current_price', ''),
                dvd.get('amazon_average_price', ''),
                dvd.get('description', ''),
                ', '.join((dvd.get('cast_and_crew', {})).get('Director', [])),
                ', '.join((dvd.get('cast_and_crew', {})).get('Writer', [])),
                ', '.join((dvd.get('cast_and_crew', {})).get('Cast', [])),
                ', '.join((dvd.get('cast_and_crew', {})).get('Producer', [])),
                dvd.get('blu_ray_url', ''),
                dvd.get('', ''),
                dvd.get('front_url', ''),
                dvd.get('back_url', ''),
                dvd.get('slip_url', ''),
                dvd.get('slipback_url', ''),
                dvd.get('overview_url', ''),
                ','.join(dvd.get('screenshot_urls', [])),
            ]

            ws.append(row)

    file_name = f'4K excels/4K-{countries_map[country]}.xlsx'
    wb.save(file_name)
    print(f'Data successfully written to {file_name}')

if __name__ == '__main__':

    countries = ['us', 'uk', 'ca', 'au', 'tr', 'za', 'id', 'mx', 'my', 'ph', 'sg', 'no', 'ua', 'ae', 'lv', 'lt', 'kr', 'it', 'in', 'il', 'ie', 'de', 'fr', 'es', 'ar', 'at', 'be', 'bg', "br" ,  'cn', 'cz', 'ee', 'fi', 'hu', 'is', 'co', 'th', 'ch', 'cl', 'ro', 'nz', 'ru', 'ru', 'gr', 'hk', 'nl', 'tw', 'jp', 'pt', 'pl', 'se','dk']

    countries_map = {
        'us':'USA',
        'uk':'United Kingdom',
        'ca':'Canada',
        'au':'Australia',
        'tr':'Turkey',
        'za':'South Africa',
        'id':'Indonesia',
        'mx':'Mexico',
        'my':'Malaysia',
        'ph':'Philippines',
        'sg':'Singapore',
        'no':'Norway',
        'ua':'Ukraine',
        'ae':'United Arab Emirates',
        'lv':'Latvia',
        'lt':'Lithuania',
        'kr':'South Korea',
        'it':'Italy',
        'in':'India',
        'il':'Israel',
        'ie':'Ireland',
        'de':'Germany',
        'fr':'France',
        'es':'Spain',
        'ar':'Argentina',
        'at':'Austria',
        'be':'Belgium',
        'bg':'Bulgaria',
        'br':'Brazil',
        'cn':'China',
        'cz':'Czech Republic',
        'ee':'Estonia',
        'fi':'Finland',
        'hu':'Hungary',
        'is':'Iceland',
        'co':'Colombia',
        'th':'Thailand',
        'ch':'Switzerland',
        'cl':'Chile',
        'ro':'Romania',
        'nz':'New Zealand',
        'ru':'Russia',
        'gr':'Greece',
        'hk':'Hong Kong',
        'nl':'Netherlands',
        'tw':'Taiwan',
        'jp':'Japan',
        'pt':'Portugal',
        'pl':'Poland',
        'se':'Sweden',
        'dk':'Denmark',
    }
    
    countries = ['fr']
    for country in countries:
        try:
            with open(f'data/4K-{country}.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            write_data_to_file(data, country, countries_map)
        except Exception as e:
            print(e)
            pass