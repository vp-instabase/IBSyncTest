from instabase.ocr.client.libs import ibocr
import logging
import re
import json
import requests
from datetime import datetime
import traceback

CONFIG_PATH = 'Corporate_Institutional_Banking/Trade_AOF/fs/Instabase Drive/Trade_AOF/config/config.json'
CONFIG_DATA = {}
JOB_ID_PATTERN = r'(47_[0-9]*[a-zA-Z]*)/*'

def update_db(col, INPUT_FILEPATH):
  try:
    doc_id = ''.join((INPUT_FILEPATH.split('/')[-1]).split('.')[:-1])
    if re.search(JOB_ID_PATTERN, INPUT_FILEPATH):
      jobid = re.search(JOB_ID_PATTERN, INPUT_FILEPATH).group()
      jobid = jobid.replace('/', '')
      query = "update tradeaof_reports set {column}=sysdate where documentid='{docid}' and jobid='{jobid}'"
      query = query.format(column=col, docid=doc_id, jobid=jobid)
      resp = requests.post(CONFIG_DATA['DB_URL_SSL'], json={"query": query, "action": "update"}, verify=False)
  except Exception as ex:
    logging.info('Error updating db: ' + str(traceback.format_exc()))


def getbarcode(INPUT_COL, INPUT_FILEPATH,ROOT_OUTPUT_FOLDER, CLIENTS, **kwargs):
  # Read config file
  data, _ = CLIENTS.ibfile.read_file(CONFIG_PATH)
  global CONFIG_DATA
  CONFIG_DATA = json.loads(data)
  update_db('udf1starttime', INPUT_FILEPATH)
  logging.info('config data' + str(CONFIG_DATA))
  logging.info('file path: ' + INPUT_FILEPATH)
  builder, err = ibocr.ParsedIBOCRBuilder.load_from_str(INPUT_FILEPATH, INPUT_COL)
  doc_id = ''.join((INPUT_FILEPATH.split('/')[-1]).split('.')[:-1])
  logging.info('docid' + doc_id)
  document = { "documentid" : doc_id, "barcodeDetails": [], "extractionDetails": {}}  
  barcodes = document["barcodeDetails"]
  # Read other fields from refiner document
  refiner_op = read_refiner(INPUT_COL, INPUT_FILEPATH)
  logging.info('*******Refiner Output****' + str(refiner_op))

  # Build response for other fields
  currency_and_amount = refiner_op.get('Currency_and_amount', '').split(' ')
  currency, amount = "", ""
  currency_and_amount = list(map(lambda x: '' if x=='ERROR' else x, currency_and_amount))
  if len(currency_and_amount) > 1:
    currency = currency_and_amount[0]
    amount = currency_and_amount[1]
  elif len(currency_and_amount)==1:
    amount = currency_and_amount[0]
  
  _d = document["extractionDetails"]
  _d["amount"] = amount
  _d["currency"] = currency
  _d["clientid"] = "" if refiner_op.get('Client_ID', "") == 'ERROR' else refiner_op.get('Client_ID', "")
  _d["product"] = ""
  _d["country"] = ""

  if builder:
    counter = 1
    for record in builder.get_ibocr_records():
      temp = {"barcode": []}
      for row in record.get_barcodes():
        temp["page"] = row['page'] + 1
        temp["barcode"].append({
          "barcodeValue": row["text"],
          "barcodeType": row["type"]
        })
        counter += 1
      # temp["barcode"] = temp["barcode"][::-1]
      barcodes.append(temp)
  update_db('udf1endtime', INPUT_FILEPATH)
  return json.dumps(document)


def read_refiner(INPUT_COL,INPUT_FILEPATH,**kwargs):
  logging.info('starting of the read_refiner script')
  builder, err = ibocr.ParsedIBOCRBuilder.load_from_str(INPUT_FILEPATH, INPUT_COL)
  if builder:
    for record in builder.get_ibocr_records():
      phrases, err = record.get_refined_phrases()
      dct = {}
      if phrases:
        for phrase in phrases:
          dct[phrase.get_column_name()] = phrase.get_column_value()
  return dct
  

def register(name_to_fn):
  name_to_fn.update({
    'get_barcode': {
      'fn': getbarcode,
      'ex': '',
      'desc': ''
    },
    'read_refiner': {
      'fn': read_refiner,
      'ex': '',
      'desc': ''
    }
  })
