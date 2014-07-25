"""Utils for working with the parquet thrift models"""

from ttypes import FieldRepetitionType, SchemaElement, Type

class SchemaParser(object):
    TYPE_MAPPING = {
        'string' : 'byte_array'
    }
    def __init__(self, type_mapping = None):
        self._schema_elements = []
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
        self._match("{", tokens)
        while not self._la1("}", tokens) and len(tokens)>1:
            rt = FieldRepetitionType._NAMES_TO_VALUES[tokens.pop(0).upper()]
            t = tokens.pop(0)
            n = tokens.pop(0)
            element = SchemaElement(name = n, repetition_type = rt)
            self._schema_elements.append(element)
            if t != "group":
                assert self._match(";", tokens), "expect ';'"
                element.type = self._map_type(t)
            else:
                id = len(self._schema_elements)
                self._parse_grp(tokens)
                nid = len(self._schema_elements)
                element.num_children = nid - id;
        self._match("}", tokens)

    def parse(self, text):
        tokens = self._lex(text)
        assert self._match("message", tokens)
        doc = tokens.pop(0)
        self._schema_elements = []
        if self._la1("{", tokens):
            self._parse_grp(tokens)
        return self._schema_elements

    def dump(self):
        for id,element in enumerate(self._schema_elements):
            print id,element

class SchemaHelper(object):
    ROOT_NODE = -1

    def __init__(self, schema_elements):
        self.schema_elements = schema_elements
        self._parent_to_child = {}
        self._child_to_parent = {}
        self._rep_level = range(0, len(self.schema_elements))
        self._def_level = range(0, len(self.schema_elements))
        self._element_path = [[] for i in range(0, len(self.schema_elements))]
        top_child = self._peer_schema_elements(0, len(self.schema_elements))
        self._rebuild_tree(top_child, SchemaHelper.ROOT_NODE, 0, 0)
        self._path_to_id = dict([('.'.join(self._element_path[id]), id)
            for id in range(0, len(self.schema_elements))])

    def schema_element(self, name):
        """Get the schema element with the given name."""
        return self.schema_elements_by_name[name]

    def is_required(self, name):
        """Returns true iff the schema element with the given name is
        required"""
        return self.schema_element(name).repetition_type == FieldRepetitionType.REQUIRED

    def max_repetition_level(self, path):
        """get the max repetition level for the given schema path."""
        max_level = 0
        for part in path:
            se = self.schema_element(part)
            if se.repetition_type == FieldRepetitionType.REQUIRED:
                max_level += 1
        return max_level

    def max_definition_level(self, path):
        """get the max definition level for the given schema path."""
        max_level = 0
        for part in path:
            se = self.schema_element(part)
            if se.repetition_type != FieldRepetitionType.REQUIRED:
                max_level += 1
        return max_level

    def _peer_schema_elements(self, fid, limit):
        field_ids = []
        while fid < limit:
            field_ids.append(fid)
            element = self.schema_elements[fid]
            if element.num_children != None:
                fid += element.num_children
            fid += 1
        return field_ids
    
    def _rebuild_tree(self, children, parent, parent_rep_level, parent_def_level):
        self._parent_to_child[parent] = children
        for chd in children:
            self._child_to_parent[chd] = parent
            chd_rep_level = parent_rep_level
            chd_def_level = parent_def_level
            element = self.schema_elements[chd]
            if element.repetition_type == FieldRepetitionType.REPEATED:
                chd_rep_level += 1
            if element.repetition_type != FieldRepetitionType.REQUIRED:
                chd_def_level += 1
            self._rep_level[chd] = chd_rep_level
            self._def_level[chd] = chd_def_level
            
            if parent != SchemaHelper.ROOT_NODE:
                self._element_path[chd] = self._element_path[parent][:]
            self._element_path[chd].append(element.name)
            if element.num_children != None:
                grand_chd = self._peer_schema_elements(chd + 1, 
                    chd + 1 + element.num_children)
                self._rebuild_tree(grand_chd, chd, chd_rep_level, chd_def_level)
    
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
        self._edges = [{} for x in self.schema_elements]
        self._build_child_fsm(SchemaHelper.ROOT_NODE)
    
    def _build_child_fsm(self, parent_id):
        child_ids = self.children(parent_id)
        for i,id in enumerate(child_ids):
            element = self.schema_elements[id]
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
            element = self.schema_elements[ts_id]
            while element.num_children != None:
                ts_id = ts_id + 1
                element = self.schema_elements[ts_id]
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
            element = self.schema_elements[id]
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
        self.column_readers = column_readers
    
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
            print r,d,'{0} -> {1}'.format(fid, nfid)
            fid = nfid
            if fid != -1:
                rd = self.column_readers[fid]

    def dump(self):
        print self._schema_helper._path_to_id
        for id, element in enumerate(self.schema_elements):
            print id, element
        for id, edge in enumerate(self._schema_helper._edges):
            print id, edge

