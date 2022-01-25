import psycopg2, logging
from psycopg2.extras import LoggingConnection
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import ConfigParser
import os
from datetime import datetime
import time

logger = logging.getLogger(__name__)

beginning = datetime.now()

query = "select so.name as \"Order name\", so.date_order, pp.name_template as \"Product name\", pp.default_code, rc.name, rp.email, fs.name, so.id from sale_order so join stock_picking sp on sp.origin like so.name || '%' join stock_move sm on sm.picking_id = sp.id join product_product pp on sm.product_id = pp.id join res_partner rp on rp.id = so.partner_id join res_country rc on rp.country_id = rc.id join fulfillment_statuses fs on so.fulfillment_status = fs.id where so.fulfillment_status in (1, 22, 13, 24, 62, 60, 70) and so.warehouse_id = 2 and so.state not in ('cancel') and sm.state in ('waiting', 'confirmed') order by date_order asc"

cred = ConfigParser.ConfigParser()
cred.readfp(open(os.path.expanduser('~/stilnest-influencer-db/credentials.ini')))
odoo_user = cred._defaults['odoo_user'].encode('ascii')
odoo_db = cred._defaults['odoo_dbname'].encode('ascii')
odoo_host = cred._defaults['odoo_host'].encode('ascii')
odoo_password = cred._defaults['odoo_password'].encode('ascii')
client_id = cred._defaults['gsheets_client_id'].encode('ascii')
client_secret = cred._defaults['gsheets_client_secret'].encode('ascii')

db_settings = {'user': odoo_user,
               'password': odoo_password,
               'host': odoo_host,
               'database': odoo_db}

today = datetime.today()

conn = psycopg2.connect(connection_factory=LoggingConnection, **db_settings)
conn.initialize(logger)
with conn.cursor() as cur:
    cur.execute(query, )
    open_orders = cur.fetchall()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

creds = ServiceAccountCredentials.from_json_keyfile_name('service_account_google.json', scope)

client = gspread.authorize(creds)
doc = client.open_by_key("1jHyenK9-ftQdz6sdp_1M9BWxQhO8hvT6frEAjMumVJ4")
name_new_sheet = "DK %s %s %s" % (today.day, today.month, today.year)
sheet = doc.add_worksheet(title=name_new_sheet, rows="1", cols="6")

headers = ['Order name', 'Date order', 'Product name', 'SKU', 'Country', 'customer email', 'Fulfilment status',
           'Purchase Order']
sheet.append_row(headers)

i = 0
for row in open_orders:
    i += 1
    if i == 90:
        time.sleep(100)
        i = 0
    aux = list(row)
    query = """SELECT  *
                                        FROM procurement_group  pg
                                        LEFT JOIN procurement_order po
                                            ON pg.id = po.group_id
                                        where pg.name = %s
                                        """

    with conn.cursor() as cur:
        cur.execute(query, (aux[0],))
        reference = cur.fetchall()
    for ref in reference:
        if ref[34] != None:
            query = """SELECT  po.name FROM purchase_order_line pl
                                        LEFT JOIN purchase_order po
                                            ON po.id = pl.order_id
                                            where pl.id = %s
                                                   """
            with conn.cursor() as cur:
                cur.execute(query, (ref[34],))
                reference = cur.fetchall()
                aux.append(reference[0][0])

    aux[1] = aux[1].strftime("%y-%m-%d %H:%M:%S")
    try:
        sheet.append_row(aux)
    except gspread.exceptions.APIError as exc:
        reset = False
        while reset is False:
            print("Waiting for API quota to reset")
            time.sleep(60)
            try:
                sheet.append_row(aux)
                reset = True
            except gspread.exceptions.APIError as exc:
                if exc.response.json()['error']['status'] == 'RESOURCE_EXHAUSTED':
                    continue
                else:
                    raise exc

end = datetime.now()
