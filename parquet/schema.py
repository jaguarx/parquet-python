"""Utils for working with the parquet thrift models"""

from ttypes import FieldRepetitionType, SchemaElement, Type

def _check_header_magic_bytes(fo):
    "Returns true if the file-like obj has the PAR1 magic bytes at the header"
    fo.seek(0, 0)
    magic = fo.read(4)
    return magic == 'PAR1'


def _check_footer_magic_bytes(fo):
    "Returns true if the file-like obj has the PAR1 magic bytes at the footer"
    fo.seek(-4, 2)  # seek to four bytes from the end of the file
    magic = fo.read(4)
    return magic == 'PAR1'


def _get_footer_size(fo):
    "Readers the footer size in bytes, which is serialized as little endian"
    fo.seek(-8, 2)
    tup = struct.unpack("<i", fo.read(4))
    return tup[0]


def _read_footer(fo):
    """Reads the footer from the given file object, returning a FileMetaData
    object. This method assumes that the fo references a valid parquet file"""
    footer_size = _get_footer_size(fo)
    logger.debug("Footer size in bytes: %s", footer_size)
    fo.seek(-(8 + footer_size), 2)  # seek to beginning of footer
    tin = TTransport.TFileObjectTransport(fo)
    pin = TCompactProtocol.TCompactProtocol(tin)
    fmd = FileMetaData()
    fmd.read(pin)
    return fmd

def read_footer(filename):
    """Reads and returns the FileMetaData object for the given file."""
    with open(filename, 'rb') as fo:
        if not _check_header_magic_bytes(fo) or \
           not _check_footer_magic_bytes(fo):
            raise ParquetFormatException("{0} is not a valid parquet file "
                                         "(missing magic bytes)"
                                         .format(filename))
        return _read_footer(fo)


class SchemaParser(object):
    TYPE_MAPPING = {
        'string' : 'byte_array'
    }
    def __init__(self, type_mapping = None):
        self.schema_elements = []
        if type_mapping != None:
            self.type_mapping = type_mapping
        else:
            self.type_mapping = SchemaParser.TYPE_MAPPING

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
                    t = []
                tokens.append(c)
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

	def _parse_type(self, type_string):
		return Type._NAMES_TO_VALUES[type_string]

    def _map_type(self, type):
        if type in self.type_mapping:
            type = self.type_mapping[type]
        return Type._NAMES_TO_VALUES[type.upper()]

    def _parse_grp(self, tokens):
        g = []
        self._match("{", tokens)
        while not self._la1("}", tokens) and len(tokens)>1:
            rt = FieldRepetitionType._NAMES_TO_VALUES[tokens.pop(0).upper()]
            t = tokens.pop(0)
            n = tokens.pop(0)
            element = SchemaElement(name = n, repetition_type = rt)
            self.schema_elements.append(element)
            g.append(element)
            if t != "group":
                assert self._match(";", tokens), "expect ';'"
                element.type = self._map_type(t)
            else:
                subg = self._parse_grp(tokens)
                element.num_children = len(subg);
        self._match("}", tokens)
        return g

    def parse(self, text):
        tokens = self._lex(text)
        assert self._match("message", tokens)
        n = tokens.pop(0)
        element = SchemaElement(name = n)
        self.schema_elements = [element]
        if self._la1("{", tokens):
            g = self._parse_grp(tokens)
            element.num_children = len(g)
        return self.schema_elements

    def dump(self):
        for id,element in enumerate(self.schema_elements):
            print id,element

    def load_from_file(self, filename):
        fmd = read_footer(filename)
        self.schema_elements = fmd.schema
        return self.schema_elements

def read_footer(filename):
    """Reads and returns the FileMetaData object for the given file."""
    with open(filename, 'rb') as fo:
        if not _check_header_magic_bytes(fo) or \
           not _check_footer_magic_bytes(fo):
            raise ParquetFormatException("{0} is not a valid parquet file "
                                         "(missing magic bytes)"
                                         .format(filename))
        return _read_footer(fo)


class SchemaHelper(object):
    ROOT_NODE = 0

    def __init__(self, schema_elements):
        self._schema_elements = schema_elements
        self._parent_to_child = {}
        self._child_to_parent = {}
        self._rep_level = range(0, len(self._schema_elements))
        self._def_level = range(0, len(self._schema_elements))
        self._element_path = [[] for i in range(0, len(self._schema_elements))]

        self._rebuild_tree(SchemaHelper.ROOT_NODE, 0, 0, [])
        self._path_to_id = dict([('.'.join(self._element_path[id]), id)
            for id in range(0, len(self._schema_elements))])

    def dump(self):
        for id,e in enumerate(self._schema_elements):
            print id,self._rep_level[id],self._def_level[id],e
        for id,p in enumerate(self._element_path):
            print id,p
        for p,id in self._path_to_id.items():
            print p,id
        for c,p in self._child_to_parent.items():
            print c,p
        for c,p in self._parent_to_child.items():
            print c,p

    def schema_element(self, name):
        """Get the schema element with the given name."""
        id = self._path_to_id[name]
        return self._schema_elements[id]

    def is_required(self, name):
        """Returns true iff the schema element with the given name is
        required"""
        return self.schema_element(name).repetition_type == FieldRepetitionType.REQUIRED

    def max_repetition_level(self, path):
        """get the max repetition level for the given schema path."""
        id = self._path_to_id[path]
        return self._rep_level[id]

    def max_definition_level(self, path):
        """get the max definition level for the given schema path."""
        id = self._path_to_id[path]
        return self._def_level[id]

    def _rebuild_tree(self, fid, rep_level, def_level, path):
        parent = self._schema_elements[fid]
        if fid != SchemaHelper.ROOT_NODE:
            if parent.repetition_type == FieldRepetitionType.REPEATED:
                rep_level += 1
            if parent.repetition_type != FieldRepetitionType.REQUIRED:
                def_level += 1
            ppath = path[:]
            ppath.append(parent.name)
            self._element_path[fid] = ppath
        self._rep_level[fid] = rep_level
        self._def_level[fid] = def_level
        if parent.num_children == None:
            return 1
        num_children = parent.num_children
        chd = fid + 1
        while num_children > 0:
            num_children -= 1
            self._child_to_parent[chd] = fid
            if not fid in self._parent_to_child.keys():
                self._parent_to_child[fid] = [chd]
            else:
                self._parent_to_child[fid].append(chd)
            chd += self._rebuild_tree(chd, rep_level, def_level,
                self._element_path[fid])
        return chd - fid
    
    def children(self, parent_id):
        return self._parent_to_child[parent_id]
    
    def parent(self, child_id):
        return self._child_to_parent[child_id]
    
    def repetition_level(self, field_id):
        return self._rep_level[field_id]
    
    def definition_level(self, field_id):
        return self._def_level[field_id]
    
    def path_name(self, field_id):
        return self._element_path[field_id]
    
    def build_full_fsm(self):
        self._edges = [{} for x in self._schema_elements]
        self._build_child_fsm(SchemaHelper.ROOT_NODE)
    
    def _build_child_fsm(self, parent_id):
        child_ids = self.children(parent_id)
        for i,id in enumerate(child_ids):
            element = self._schema_elements[id]
            rep_level = self._rep_level[id]
            edge = range(0, rep_level+1)
            for r in range(0, rep_level+1):
                if i == len(child_ids) - 1:
                    if parent_id == SchemaHelper.ROOT_NODE:
                        edge[r] = [parent_id, ' ']
                    else:
                        edge[r] = [parent_id, 'F']
                else:
                    edge[r] = [child_ids[i+1], ' ']
            if element.repetition_type == FieldRepetitionType.REPEATED:
                edge[rep_level] = [id, ' ']
            self._edges[id] = edge
            if element.num_children != None:
                self._build_child_fsm(id)
    
    def _follow_fsm(self, field_id, rep_lvl):
        edge = self._edges[field_id]
        ts = edge[rep_lvl]
        ts_id = ts[0]
        ts_m = ts[1]
        if ts_id == SchemaHelper.ROOT_NODE:
            return ts_id
        if ts_m == 'F':
            return self._follow_fsm(ts_id, rep_lvl)
        else:
            element = self._schema_elements[ts_id]
            while element.num_children != None:
                ts_id = ts_id + 1
                element = self._schema_elements[ts_id]
            return ts_id
    
    def compress_state(self, field_id, rep_lvl, fields):
        if not field_id in fields:
            return SchemaHelper.ROOT_NODE
        tsid = self._follow_fsm(field_id, rep_lvl)
        while (tsid != SchemaHelper.ROOT_NODE) and (not tsid in fields):
            field_id = tsid
            tsid = self._follow_fsm(field_id, rep_lvl)
        return tsid
    
    def compress_fsm(self, fields):
        fsm = {}
        for id in fields:
            element = self._schema_elements[id]
            edge = {}
            rep_lvl = self._rep_level[id]
            for r in range(0, rep_lvl + 1):
                tsid = self.compress_state(id, r, fields)
                edge[r] = tsid
            fsm[id] = edge
        fsm[SchemaHelper.ROOT_NODE] = fields[0]
        return fsm

class RecordDissector(object):

    def __init__(self, schema_elements, emitter = None):
        self.schema_elements = schema_elements
        self._schema_helper = SchemaHelper(schema_elements)
        self._emitter = emitter
    
    def dissect(self, record):
        field_ids = self._schema_helper.children(SchemaHelper.ROOT_NODE)
        self._dissect(record, field_ids, 0, 0)
    
    def _dissect(self, record, field_ids, rep_level, def_level):
        chd_def_level = def_level
        if record != None:
            chd_def_level += 1
        for fid in field_ids:
            element = self.schema_elements[fid]
            name = element.name
            sv = None
            if record != None and name in record.keys():
                sv = record[name]
            if element.repetition_type == FieldRepetitionType.REPEATED:
                if sv == None:
                    if element.num_children != None:
                        sub_field_ids = self._schema_helper.children(fid)
                        self._dissect(None, sub_field_ids, rep_level, chd_def_level)
                    else:
                        self._emit_field(fid, rep_level, chd_def_level, None)
                    continue
                i = 0
                chd_rep_level = rep_level
                for sv1 in sv:
                    if element.num_children != None:
                        sub_field_ids = self._schema_helper.children(fid)
                        self._dissect(sv1, sub_field_ids, chd_rep_level, chd_def_level)
                    else:
                        if sv1 != None:
                            chd_def_level = self._schema_helper.definition_level(fid)
                        self._emit_field(fid, chd_rep_level, chd_def_level, sv1)
                    if i == 0:
                        chd_rep_level = self._schema_helper.repetition_level(fid)
                    i += 1
            else:
                if element.num_children != None:
                    sub_field_ids = self._schema_helper.children(fid)
                    self._dissect(sv, sub_field_ids, rep_level, chd_def_level)
                else:
                    if sv != None:
                        chd_def_level = self._schema_helper.definition_level(fid)
                    self._emit_field(fid, rep_level, chd_def_level, sv)
    
    def _emit_field(self, fid, rep_level, def_level, value):
        if self._emitter == None:
            print "{0:30s} {1},{2}:{3}".format('.'.join(self._schema_helper.path_name(fid)),
                rep_level, def_level, value)
        else:
            self._emitter.emit(fid, rep_level, def_level, value)

#column_reader is expect to expose
#  repetition_level
#  definition_level
#  consume
class RecordAssembler(object):

    def __init__(self, schema_elements, column_readers):
        self.schema_elements = schema_elements
        self._schema_helper = SchemaHelper(schema_elements)
        self._schema_helper.build_full_fsm()
        self.column_readers = {}
        for p,id in self._schema_helper._path_to_id.items():
            if p in column_readers.keys():
                self.column_readers[id] = column_readers[p]
    
    def select_fields(self, field_paths = None):
        fields = []
        if field_paths != None:
            for path in field_paths:
                id = self._schema_helper._path_to_id[path]
                fields.append(id)
        else:
            for id,element in enumerate(self.schema_elements):
                if element.num_children == None:
                    fields.append(id)
        self._fsm = self._schema_helper.compress_fsm(fields)
        return self._fsm

    def assemble(self):
        fid = self._fsm[SchemaHelper.ROOT_NODE]
        rd = self.column_readers[fid]
        while fid != SchemaHelper.ROOT_NODE:
            d = rd.definition_level
            rd.consume()

            #if d == None:
            r = rd.repetition_level
            nfid = self._fsm[fid][r]
            #print r,d,'{0} -> {1}'.format(fid, nfid)
            fid = nfid
            if fid != SchemaHelper.ROOT_NODE:
                rd = self.column_readers[fid]

    def dump(self):
        print self._schema_helper._path_to_id
        for id, element in enumerate(self.schema_elements):
            print id, element
        for id, edge in enumerate(self._schema_helper._edges):
            print id, edge

