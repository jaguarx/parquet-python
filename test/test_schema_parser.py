#!/usr/bin/python

import unittest

from parquet.schema import SchemaParser, SchemaHelper, RecordAssembler, RecordDissector

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

field_values = {
  	'DocId': [
		[0,0,10],
		[0,0,20]
  	],
  	'Links.Backward': [
		[0,1,None],
		[0,2,10],
		[1,2,30]
  	],
  	'Links.Forward': [
		[0,2,20],
		[1,2,40],
		[1,2,60],
		[0,2,80]
	],
	'Name.Language.Code': [
		[0,2,"en-us"],
		[2,2,"en"],
		[1,1,None],
		[1,2,"en-gb"],
		[0,1,None]
	],
	'Name.Language.Country': [
		[0,3,"us"],
		[2,2,None],
		[1,1,None],
		[1,3,"gb"],
		[0,1,None],
	],
	'Name.Url': [
		[0,2,"http://A"],
		[1,2,"http://B"],
		[1,1,None],
		[0,2,"http://C"],
	]
}

schema_text = """
  message Document {
  required int64 DocId;
  optional group Links {
    repeated int64 Backward;
    repeated int64 Forward;
  }
  repeated group Name {
    repeated group Language {
      required string Code;
      optional string Country;
    }
    optional string Url; 
  }
}
"""

class TestSchemaParser(unittest.TestCase):

    def test_parser(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        self.assertTrue(len(s) == 10)
        print "test_parser done"


class FieldEmitter:

	def __init__(self, schema_elements):
		self.schema_elements = schema_elements
		self.values = []

	def emit(self, fid, rep_lvl, def_lvl, value):
		#print "[{0},{1},{2},{3}]".format(fid,rep_lvl,def_lvl,value)
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
        print "test_dissect done"

class ListReader:
	def __init__(self, id, values):
		self.values = values
		self.id = id
		self.pos = 0
		self.repetition_level = self.values[0][0]
		self.definition_level = self.values[0][1]

	def dump(self):
		print self.id,self.values

	def consume(self):
		if self.pos + 1 < len(self.values):
			self.pos += 1
			self.repetition_level = self.values[self.pos][0]
			self.definition_level = self.values[self.pos][1]
		else:
			self.repetition_level = 0
			self.definition_level = 0


class TestRecordAssemble(unittest.TestCase):

    def test_partials_fsm(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s, {})
        fsm = ra.select_fields(('DocId', 'Name.Language.Country'))
        count = 0
        for s,n in fsm.items():
        	if s != SchemaHelper.ROOT_NODE:
	        	count += len(n.keys())
        self.assertTrue( count == 4);
        print "test_partial_fsm done"

    def test_full_fsm(self):
        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s, {})
        fsm = ra.select_fields()
        count = 0
        for s,n in fsm.items():
        	if s != SchemaHelper.ROOT_NODE:
	        	count += len(n.keys())
        self.assertTrue( count == 13)
        print "test_full_fsm done"
        
    def test_partial_assemble(self):
        column_readers = dict([(id,ListReader(id,vl)) for id,vl in field_values.items()])

        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s, column_readers)
        fsm = ra.select_fields(('DocId', 'Name.Language.Country'))
        ra.assemble()
        ra.assemble()
        print "test_partial_assemble done"

    def test_full_assemble(self):
        column_readers = dict([(id,ListReader(id,vl)) for id,vl in field_values.items()])
        p = SchemaParser()
        s = p.parse(schema_text)
        ra = RecordAssembler(s, column_readers)
        fsm = ra.select_fields()
        ra.assemble()
        ra.assemble()
        print "test_full_assemble done"

if __name__ == '__main__':
    unittest.main()
