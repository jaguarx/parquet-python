#!/usr/bin/python

import unittest

from parquet.schema import SchemaParser, RecordAssembler, RecordDissector

d1 = {
  'DocId': 10,
  'Links': {
    'Forward': [20, 40, 60]
  },
  'Name': [
    {'Language':[
	  {'Code': 'en-us', 'Country': 'us'},
	  {"Code": "en"}
	  ],
	 'Url': 'http://A'},
	{'Url': 'http://B'},
	{'Language':[
	  {'Code': 'en-gb', 'Country': 'gb'}
	]}
  ]
}

d2 = {
  'DocId': 20,
  'Links': {
    'Backward': [10, 30],
    'Forward': [80]
  },
  'Name': [
    {'Url': 'http://C'}
  ]
}

schema_text = """
  message Document {
  required int64 DocId;
  optional group Links {
    repeated int64 Backward;
    repeated int64 Forward; }
  repeated group Name {
    repeated group Language {
      required string Code;
      optional string Country; }
    optional string Url; }}
}
"""

class TestSchemaParser(unittest.TestCase):

    def test_parser(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        self.assertTrue(len(s) == 9)


class FieldEmitter:

	def __init__(self, schema_elements):
		self.schema_elements = schema_elements
		self.values = []

	def emit(self, fid, rep_lvl, def_lvl, value):
		self.values.append([fid, rep_lvl, def_lvl, value])

class TestRecordDissector(unittest.TestCase):

    def test_dissect(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        emitter = FieldEmitter(s)
        rd = RecordDissector(s, emitter)
        rd.dissect(d1)
        rd.dissect(d2)
        self.assertTrue(23 == len(emitter.values))
		
class TestRecordAssemble(unittest.TestCase):

    def test_partails_fsm(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s)
        fsm = ra.select_fields(('DocId', 'Name.Language.Country'))
        count = 0
        for s,n in fsm.items():
        	count += len(n.keys())
        self.assertTrue( count == 4);

    def test_full_fsm(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s)
        fsm = ra.select_fields()
        count = 0
        for s,n in fsm.items():
        	count += len(n.keys())
        self.assertTrue( count == 13);

if __name__ == '__main__':
    unittest.main()
