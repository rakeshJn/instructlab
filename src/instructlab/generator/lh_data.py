import os
import daft
import xxhash
import requests
import time

from pyiceberg import catalog
from pyiceberg.schema import Schema
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    TimestampType,
    TimestamptzType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)

cat = catalog.load_catalog("default")

#cat.create_namespace("test1")
#nss = cat.list_namespaces()
#print(nss)

def create_table(table_name):
  table_name = table_name.replace("-", "_")
  try:
    table = cat.load_table(table_name)
    return
  except NoSuchTableError:
    print("Table doesn't exist, creating...")

  schema = Schema(
    NestedField(field_id=2, name="system", field_type=StringType(), required=False),
    NestedField(field_id=3, name="user", field_type=StringType(), required=False),
    NestedField(field_id=4, name="assistant", field_type=StringType(), required=False),
    NestedField(field_id=4, name="taxonomy_path", field_type=StringType(), required=False),
    NestedField(field_id=5, name="generated_at", field_type=TimestamptzType(), required=True),
    NestedField(field_id=1, name="doc_id", field_type=FloatType(), required=True),
  )
  # Sort on the generated_at
  sort_order = SortOrder(SortField(source_id=5, transform=IdentityTransform(), direction=SortDirection.DESC))
  cat.create_table(
    identifier=table_name,
    schema=schema,
    location=f"s3://dmf-bucket-3988900a-8936-41d7-b7c4-50ebb5250602/tables/{table_name}",
    sort_order=sort_order,
  )
  time.sleep(102)

def create_annotation_table(table_name):
  table_name = table_name.replace("-", "_")
  try:
    table = cat.load_table(table_name)
    return
  except NoSuchTableError:
    print("Table doesn't exist, creating...")

  schema = Schema(
    NestedField(field_id=2, name="entity_group", field_type=StringType(), required=False),
    NestedField(field_id=3, name="text", field_type=StringType(), required=False),
    NestedField(field_id=4, name="entity", field_type=StringType(), required=False),
    NestedField(field_id=5, name="score", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="meta", field_type=StringType(), required=False),
    NestedField(field_id=4, name="taxonomy_path", field_type=StringType(), required=False),
    NestedField(field_id=6, name="generated_at", field_type=TimestamptzType(), required=True),
    #NestedField(field_id=7, name="span", field_type=StringType(), required=False),
    NestedField(field_id=1, name="doc_id", field_type=FloatType(), required=True),
  )
  # Sort on the generated_at
  sort_order = SortOrder(SortField(source_id=6, transform=IdentityTransform(), direction=SortDirection.DESC))
  cat.create_table(
    identifier=table_name,
    schema=schema,
    location=f"s3://dmf-bucket-3988900a-8936-41d7-b7c4-50ebb5250602/tables/{table_name}",
    sort_order=sort_order,
  )
  time.sleep(102)

@daft.udf(return_dtype=daft.DataType.python())
def hash_data(data):
  ret = []
  for text in data.to_pylist():
    id = xxhash.xxh64(text, seed=42).intdigest()
    ret.append(float(id))
  return ret

def append(df, table_name):
  table = cat.load_table(table_name)
  return df.write_iceberg(table)

@daft.udf(return_dtype=daft.DataType.python())
def explode_json(jsn, field):
  ret = []
  for row in jsn.to_pylist():
    ret.append(row[field])
  return ret

@daft.udf(return_dtype=daft.DataType.python())
def annotate_udf(user, doc_id, taxonomy_path):
  user_ret = []
  #assst_ret = []
  for usr, did, t_path in zip(user.to_pylist(), doc_id.to_pylist(), taxonomy_path.to_pylist()):
    user_ret.extend(annotate(usr, did, t_path))
  return user_ret

def annotate(text: str, doc_id, taxonomy_path):
  ret =  [{
                "doc_id": doc_id,
                "taxonomy_path": taxonomy_path,
                "entity_group": ann.get("entity_group", None),
                "text": ann.get("text", ""),
                "entity": ann.get("entity", None),
                "score": ann.get("score", 0),
                "meta": ann.get("meta", {}),
                "span": get_span(ann)
         }for ann in run_annotation(text)
         ]if text else []
  return ret

def run_annotation(text):
  """Runs inference on a text string using a POST request.
  Args:
      text: The text string for inference.
  Returns:
      A dictionary containing the annotation response (JSON format).
  Raises:
      requests.exceptions.RequestException: If the request fails.
  """
  host = os.getenv('INSIGHTS_HOST')
  token = os.getenv('INSIGHTS_TOKEN')
  headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        'Connection':'close',
    }

  data = {
        "text": text,
        "parameters": {} #TODO tbd what to do with this
  }
  url = f"{host}/run"
  response = requests.post(url, headers=headers, json=data)
  response.raise_for_status()  # Raise an exception for non-2xx status codes
  return response.json()
 
def process_annotation(df2, field: str, src_table: str, generated_at):
  df2 = df2.with_column('annotation', annotate_udf(df2[field], df2['doc_id'], df2['taxonomy_path']))
  df2 = df2.exclude('doc_id')
  df2 = df2.exclude(field)
  df2 = df2.exclude('taxonomy_path')
  #df2.show()
  df2 = df2.with_columns({'doc_id': explode_json(df2['annotation'], 'doc_id') , 'entity_group': explode_json(df2['annotation'], 'entity_group'), 'text': explode_json(df2['annotation'], 'text'), 'entity': explode_json(df2['annotation'], 'entity'), 'score': explode_json(df2['annotation'], 'score'), 'taxonomy_path': explode_json(df2['annotation'], 'taxonomy_path'), })  # 'meta': explode_json(df2['annotation'], 'meta'), 'span': explode_json(df2['annotation'], 'span')})
  df2 = df2.with_column('generated_at', daft.lit(generated_at).cast(daft.DataType.timestamp(timeunit='us', timezone='UTC')))
  #df2.show()
  ann_tab = f'{src_table}_ann_{field}'
  ann_tab = ann_tab.lower().replace('-', '_')
  create_annotation_table(ann_tab)
  append(df2, ann_tab)

def get_span(ann):
  start = ann["start"]
  end = ann["end"]
  return {"start": start, "end": end} if start > end else {}

#print(annotate("fsdvd hi there this is shit. I am also writing another sentence which is obsenely busty."))
#create_table('test1.taxi_dataset6')
#id = hash_data("hi there testing this asadsdsfffasfasffasfas")
#print(id)
