## Script to do scraping web page on hotsite Neemu to get sku list ##

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tabulate import tabulate
import pkg_data_base as ndb
import pkg_variables as var

def main():
    # Open connection Admin Data Base and query label
    conn, cur = ndb.db_connect_open('admin')
    query = """
    select
      RedirectUrl as redirect_url
    from [site-net].dbo.banners (nolock)
    where (cast(CriadoEm as date) >= dateadd(day,{0},getdate()) and cast(CriadoEm as date) <= dateadd(day,{1},getdate()))
        and RedirectUrl like '%/hotsite/%'
    group by RedirectUrl
    """.format(var.STAGING_AD_DAYS_RANGE_INITIAL,var.STAGING_AD_DAYS_RANGE_FINAL)

    # Get results to Data Frame
    df_url = pd.read_sql(query, conn)

    # Close connection Admin data base
    ndb.db_connect_close(conn, cur)

    # Variable to limit product quantity in page
    product_limit = 1000

    # Create final data frame
    df_sku_url = pd.DataFrame(columns=['sku','url'])

    # Loop to run each url Hotsite
    for i in range(len(df_url)):
        # Considering only hotsite page
        if df_url.loc[i].str.contains('hotsite').bool:
            # Prepare url to do scraping
            url_text = "https://busca2.site-net.com.br/busca?q=hotsite"
            url_text += df_url.loc[i].str.split('hotsite/')[0][1]
            url_text += "&nm_hotsite="
            url_text += df_url.loc[i].str.split('hotsite/')[0][1]
            url_text += "&results_per_page="
            url_text += str(product_limit)

            # get html page
            res = requests.get(url_text)
            soup = BeautifulSoup(res.text, 'html.parser')
            table_html = soup.find_all('div', class_='product-item ng-scope')
            # Create data frame to receive result sku and url
            df_sku = pd.DataFrame(columns=['sku','url'])

            # Loop to split html div and get final result
            for x in range(len(table_html)):
                tb_div = table_html[x]
                df_sku.loc[x] = tb_div['id'].split('-')[1], df_url.loc[i]['redirect_url']
            # Append final results
            df_sku_url = df_sku_url.append(df_sku, sort=False)

    #Bulk loader insertion in staging
    ndb.bulk_loader_insert('groot', 'ne_scraping_hotsite', df_sku_url)

if __name__ == '__main__':
    main()
