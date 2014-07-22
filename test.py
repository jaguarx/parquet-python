#!/usr/bin/python

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

def dissect_event(e, n, r, v, d=0):
  print "{0:6s} {1:30s} {2},{3}:{4}".format(e,n,r,d,v)

def dissect_record(r, s, rl=0, dl=0):
  sdl = dl
  if r != None: sdl+=1
  for e in s:
    n = e['n']
    p = '.'.join(e['p'])
    sv = None
    if r != None and n in r.keys():
      sv = r[n]
    if e['rt'] == 'repeated':
      if sv == None:
        if 'sub' in e.keys():
          dissect_record(None, e['sub'], rl, sdl)
        else:
          dissect_event('atomic', p, rl, None, sdl)
        continue
      i = 0
      crl = rl
      for sv1 in sv:
        if 'sub' in e.keys():
          dissect_record(sv1, e['sub'], crl, sdl)
        else:
          if sv1 != None: sdl = e['d']
          dissect_event('atomic', p, crl, sv1, sdl)
        if i == 0:
		  crl = e['r']
        i += 1
    else:
      if 'sub' in e.keys():
        dissect_record(sv, e['sub'], rl, sdl)
      else:
        if sv != None: sdl = e['d']
        dissect_event('atomic', p, rl, sv, sdl)
		
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


class Schema:
  def __init__(self, text = None):
    if text != None:
      self.parse(text)

  def _lex(self, text):
    tokens = []
    t = []
    for c in text:
      if c in (' ', '\t', '\r', '\n'):
        if len(t)>0:
          tokens.append(''.join(t))
          t = []
      elif c in ('{', '}', ';'):
        if len(t)>0:
          tokens.append(''.join(t))
        tokens.append(c)
        t = []
      else:
        t.append(c)
    if len(t)>0:
      tokens.append(t)
    return tokens

  def _match(self, tm, tokens):
    t = tokens[0]
    if t == tm:
      tokens.pop(0)
      return True
    else:
      return False

  def _la1(self, tm, tokens):
    t = tokens[0]
    if t == tm:
      return True
    else:
      return False

  def _parse_grp(self, tokens):
    sub = []
    self._match("{", tokens)
    while not self._la1("}", tokens) and len(tokens)>1:
      rt = tokens.pop(0)
      t = tokens.pop(0)
      n = tokens.pop(0)
      #print rt,t,n
      if t != "group":
        assert self._match(";", tokens), "expect ';'"
        sub.append({"n":n, "t":t, "rt": rt})
      else:
        g = self._parse_grp(tokens)
        sub.append({"n":n, "t":t, "rt": rt, "sub":g})
    self._match("}", tokens)
    return sub

  def _scan_rd(self, s,r,d,p):
    for e in s:
      sr = r
      sd = d
      if e['rt'] == 'repeated':
        sr = sr + 1
      if not e['rt'] == 'required':
        sd = sd + 1
      e['r'] = sr
      e['d'] = sd
      p2 = p[:]
      p2.append(e['n'])
      e['p'] = p2
      if 'sub' in e:
        self._scan_rd(e['sub'], sr, sd, p2)
    
  def parse(self, text):
    tokens = self._lex(text)
    assert self._match("message", tokens)
    doc = tokens.pop(0)
    s = {"n": doc}
    if self._la1("{", tokens):
      s['g'] = self._parse_grp(tokens)
    self._scan_rd(s['g'], 0, 0,[])
    self._schema = s
    return s

class RecordDissector:
  def __init__(self, schema):
    self._schema = schema

  def dissect_record(self, record):
    self._dissect(record, self._schema._schema['g'])

  def _dissect(self, r, s, rl=0, dl=0):
      sdl = dl
      if r != None: sdl+=1
      for e in s:
        n = e['n']
        p = '.'.join(e['p'])
        sv = None
        if r != None and n in r.keys():
          sv = r[n]
        if e['rt'] == 'repeated':
          if sv == None:
            if 'sub' in e.keys():
              dissect_record(None, e['sub'], rl, sdl)
            else:
              dissect_event('atomic', p, rl, None, sdl)
            continue
          i = 0
          crl = rl
          for sv1 in sv:
            if 'sub' in e.keys():
              dissect_record(sv1, e['sub'], crl, sdl)
            else:
              if sv1 != None: sdl = e['d']
              dissect_event('atomic', p, crl, sv1, sdl)
            if i == 0:
              crl = e['r']
            i += 1
        else:
          if 'sub' in e.keys():
            dissect_record(sv, e['sub'], rl, sdl)
          else:
            if sv != None: sdl = e['d']
            dissect_event('atomic', p, rl, sv, sdl)

s = Schema(schema_text)
rd = RecordDissector(s)
rd.dissect_record(d1)
