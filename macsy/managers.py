import inspect
import pymongo
from macsy.utils import suppress_print_if_mocking
from datetime import datetime
from dateutil import parser as dtparser
from bson import ObjectId
from bson.codec_options import DEFAULT_CODEC_OPTIONS
codec_options = DEFAULT_CODEC_OPTIONS.with_options(unicode_decode_error_handler='ignore')

class BaseManager():

    def __init__(self, blackboard, suffix):
        from macsy.utils import QueryBuilder
        self.check_caller()
        self._blackboard = blackboard
        self._query_builder = QueryBuilder(blackboard)
        self._collection = self._blackboard._db.get_collection(self._blackboard._name + suffix, codec_options=codec_options)

    def check_caller(self):
        from macsy.blackboards import Blackboard, DateBasedBlackboard
        stack = inspect.stack()
        depth = 4 if 'DateBased' in self.__class__.__name__ else 3
        the_class = stack[depth][0].f_locals["self"].__class__
        the_method = stack[depth][0].f_code.co_name
        if (the_class is not DateBasedBlackboard.__class__ or Blackboard.__class__) and the_method is not '__init__':
            raise UserWarning('{} should not be created outside of the Blackboard class or its subclasses.'.format(self.__class__.__name__))

class CounterManager(BaseManager):

    counter_suffix = '_COUNTER'
    counter_id = '_id'
    counter_next = 'NEXT_ID'
    counter_indexes = 'INDEXES'
    counter_hash = 'HASH_FIELD'
    counter_type = 'BLACKBOARD_TYPE'
    counter_type_standard = 'STANDARD'
    counter_type_date_based = 'DATE_BASED'
    counter_tag = "tag_counter"
    counter_doc = "doc_counter"
    counter_hash_fields = 'fields'

    def __init__(self, blackboard):
        super().__init__(blackboard, CounterManager.counter_suffix)

    def get_next_id_and_increment(self, field):
        next_id = self.get_next_id(field)
        self._increment_next_id(next_id, field)
        return next_id

    def get_next_id(self, field):
        result = self._collection.find_one({CounterManager.counter_id : CounterManager.counter_next})
        return result[field]

    def get_required_indexes(self):
        result = self._collection.find_one({CounterManager.counter_id : CounterManager.counter_indexes})
        if result is not None:
            return result[CounterManager.counter_indexes]
        print('Warning: No required indexes defined for the Blackboard.')
        # fallback to ensure that ids are indexed.
        return [{self._blackboard.document_manager.doc_id : 1}]

    def get_hash_field(self):
        result = self._collection.find_one({CounterManager.counter_id : CounterManager.counter_hash})
        if result is not None:
            return result[CounterManager.counter_hash]
        # if there is no hash field defined, fallback to using HSH as a default.
        return 'HSH'

    def get_hash_components(self):
        result = self._collection.find_one({CounterManager.counter_id : CounterManager.counter_hash})
        if result is not None:
            return result[CounterManager.counter_hash_fields]
        # if there are no hash components defined, fallback to using the id field.
        return [self._blackboard.document_manager.doc_id]

    def _increment_next_id(self, current_id, field):
        next_id = {"$set" : {field : int(current_id+1)}}
        self._collection.update({CounterManager.counter_id : CounterManager.counter_next}, next_id)

class TagManager(BaseManager):

    tag_suffix = '_TAGS'
    tag_id = '_id'
    tag_name = 'Nm'
    tag_control = 'Ctrl'
    tag_inherit = 'DInh'
    control_tags = ['FOR>', 'POST>']

    def __init__(self, blackboard):
        suffix = TagManager.tag_suffix
        super().__init__(blackboard, suffix)

    def insert_tag(self, tag_name, inheritable=False):
        ctrl = 1 if any(map(tag_name.startswith, TagManager.control_tags)) else 0
        inherit = 0 if inheritable is not True else 1
        tag = {TagManager.tag_id : self._blackboard.counter_manager.get_next_id_and_increment(self._blackboard.counter_manager.counter_tag), 
            TagManager.tag_name : tag_name, 
            TagManager.tag_control : ctrl, 
            TagManager.tag_inherit : inherit}
        return self._collection.insert(tag)

    def update_tag(self, tag_id, tag_name, inheritable=None):
        tag = self.get_tag(tag_id)
        if inheritable:
            tag[TagManager.tag_inherit] = 1
        tag[TagManager.tag_name] = tag_name
        if any(map(tag_name.startswith, TagManager.control_tags)):
            tag[TagManager.tag_control] = 1
        return self._collection.update({TagManager.tag_id : tag_id}, {"$set" : tag})

    def delete_tag(self, tag_id):
        self._remove_tag_from_all(tag_id)
        return self._collection.remove({TagManager.tag_id : tag_id})

    def get_tag(self, tag_id=None, tag_name=None):
        if tag_id is not None:
            return self._collection.find_one({TagManager.tag_id : tag_id})
        else:
            return self._collection.find_one({TagManager.tag_name : tag_name})

    def get_all_tags(self):
        return self._collection.find()

    def is_control_tag(self, tag_id=None, tag_name=None):
        return self._tag_has_property(TagManager.tag_control, tag_id, tag_name)

    def is_inheritable_tag(self, tag_id=None, tag_name=None):
        return self._tag_has_property(TagManager.tag_inherit, tag_id, tag_name)

    def check_tag_type(self, tag, func):
        return func(tag_name=tag) if isinstance(tag, str) else func(tag_id=tag)

    def tag_exists(self, tag_name):
        exists = self._collection.find_one({TagManager.tag_name : tag_name})
        return True if exists is not None else False

    def _tag_has_property(self, tag_property, tag_id=None, tag_name=None):
        tag = self.get_tag(tag_id=tag_id) if tag_id is not None else self.get_tag(tag_name=tag_name)
        test = tag[tag_property] if (tag is not None and tag_property in tag) else False
        return bool(test)

    def get_canonical_tag(self, tag):
        full_tag = self.get_tag(tag_name=tag) if isinstance(tag,str) else self.get_tag(tag_id=tag)
        if full_tag is None:
            raise ValueError('Tag does not exist: {}'.format(tag))
        return full_tag

    def _remove_tag_from_all(self, tag_id):
        for doc in self._blackboard.find(tags=[tag_id]):
            self._blackboard.remove_tag(doc[self._blackboard.document_manager.doc_id], tag_id)

class DocumentManager(BaseManager):

    doc_id = '_id'
    doc_tags = 'Tg'
    doc_control_tags = 'FOR'

    def __init__(self, blackboard):
        super().__init__(blackboard, '')
        self.array_fields = [DocumentManager.doc_tags, DocumentManager.doc_control_tags]
        self.doc_id = DocumentManager.doc_id
        self.doc_tags = DocumentManager.doc_tags
        self.doc_control_tags = DocumentManager.doc_control_tags
        self._ensure_indexes(self._collection)
        
    def find(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        sort = [(self.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
        max_docs = kwargs.pop('max', 0)
        return (self._collection.find(query).sort(sort).limit(max_docs), max_docs)

    def count(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        return self._collection.find(query).count()

    def insert(self, doc):
        doc[self.doc_id] = self._get_or_generate_id(doc)
        self._ensure_array_fields(doc)
        exists, ident = self._doc_exists_and_id(doc)
        if exists:
            return self.update(ident, doc)
        else:
            doc[self._blackboard.counter_manager.get_hash_field()] = self._get_or_generate_hash(doc)
            return self._collection.insert(doc)

    def update(self, doc_id, updated_fields):
        update = self._query_builder.build_document_update(doc_id, updated_fields)
        response = self._collection.update({self.doc_id : doc_id}, update)
        return doc_id if response['updatedExisting'] else None

    def delete(self, doc_id):
        return self._collection.remove({self.doc_id : doc_id})

    def update_document_tags(self, ids, operations):
        doc_id, tag_id = ids
        update = self._get_document_tag_update(tag_id, operations)
        response = self._collection.update({self.doc_id : doc_id}, update)
        return doc_id if response['updatedExisting'] else None

    def _doc_exists_and_id(self, doc):
        hsh = self._get_or_generate_hash(doc)
        results = [x for x in self._collection.find({self._blackboard.counter_manager.get_hash_field() : hsh})]
        return (True, results[0][self.doc_id]) if results else (False, None)

    def _get_or_generate_hash(self, doc):
        from macsy import utils
        hash_field = self._blackboard.counter_manager.get_hash_field()
        if hash_field in doc:
            return doc[hash_field]
        components = self._blackboard.counter_manager.get_hash_components()
        hsh = utils.java_string_hashcode("".join([str(doc[x]) for x in components if x in doc]))
        return hsh

    def _get_or_generate_id(self, doc):
        if self.doc_id not in doc:
            return self._blackboard.counter_manager.get_next_id_and_increment(self._blackboard.counter_manager.counter_doc)
        return doc[self.doc_id]

    def _ensure_array_fields(self, doc):
        missing_tags = {field : [] for field in self.array_fields if field not in doc}
        doc.update(missing_tags)

    def _get_document_tag_update(self, tag_id, operations):
        return self._query_builder.build_tags_update_query(tag_id, operations[0]) if isinstance(tag_id, list) else \
            self._query_builder.build_tag_update_query(tag_id, operations[1])

    @suppress_print_if_mocking
    def _ensure_indexes(self, collection):
        required = self._blackboard.counter_manager.get_required_indexes()
        existing = collection.index_information()
        missing = self._find_missing_indexes(required, existing)
        for index in missing:
            print('Building {} index for {} in the background.'.format(index, collection.name))
            collection.create_index(index, background=True)

    def _find_missing_indexes(self, required, existing):
        if existing is not None:
            for index in existing:
                if existing['key'] in required:
                    required.pop(existing['key'])
        return required

class DateBasedDocumentManager(DocumentManager):

    def __init__(self, blackboard):
        super().__init__(blackboard)
        self._populate_collections()
        self.array_fields.extend(['Fds','LOC'])
        self.doc_id = DateBasedDocumentManager.doc_id
        self.doc_tags = DateBasedDocumentManager.doc_tags
        self.doc_control_tags = DateBasedDocumentManager.doc_control_tags

    def _populate_collections(self):
        colls = ((coll.split('_')[-1], coll) for coll in self._blackboard._db.collection_names() if self._blackboard._name in coll)
        self._collections = {int(year): self._blackboard._db.get_collection(coll,codec_options=codec_options) for year, coll in colls if year.isdigit()}
        self._max_year = max(self._collections.keys())
        self._min_year = min(self._collections.keys())
        for coll in self._collections.values():
            self._ensure_indexes(coll)

    def find(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        sort = [(self.doc_id, kwargs.pop('sort', pymongo.DESCENDING))]
        max_docs = kwargs.pop('max', 0)
        years = self._parse_year_range(**kwargs)
        year_range = range(years[0], years[1]+1, pymongo.ASCENDING) if bool(sort[0][1] == pymongo.ASCENDING) else range(years[1], years[0]-1, pymongo.DESCENDING)
        response = [self._collections[year].find(query).sort(sort).limit(max_docs) for year in year_range]
        return (response, max_docs)

    def count(self, **kwargs):
        query = kwargs.get('query', self._query_builder.build_document_query(**kwargs))
        min_year, max_year = self._parse_year_range(**kwargs)
        return sum([self._collections[year].find(query).count() for year in range(max_year, min_year-1, pymongo.DESCENDING)])

    def insert(self, doc):
        doc[self.doc_id] = self._get_or_generate_id(doc)
        self._ensure_array_fields(doc)
        year = self._get_doc_year(doc)
        exists, ident = self._doc_exists_and_id(doc)
        if exists:
            return self.update(ident, doc)
        else:
            doc[self._blackboard.counter_manager.get_hash_field()] = self._get_or_generate_hash(doc)
            return self._collections[year].insert(doc)

    def update(self, doc_id, updated_fields):
        update = self._query_builder.build_document_update(doc_id, updated_fields)
        year = self._get_doc_year({self.doc_id : doc_id})
        response = self._collections[year].update({self.doc_id : doc_id}, update)
        return doc_id if response['updatedExisting'] else None

    def delete(self, doc_id):
        year = self._get_doc_year({self.doc_id : doc_id})
        return self._collections[year].remove({self.doc_id : doc_id})

    def update_document_tags(self, ids, operations):
        doc_id, tag_id = ids
        year = self._get_doc_year({self.doc_id : doc_id})
        update = self._get_document_tag_update(tag_id, operations)
        response = self._collections[year].update({self.doc_id : doc_id}, update)
        return doc_id if response['updatedExisting'] else None

    def get_date(self, doc):
        if self.doc_id in doc and isinstance(doc[self.doc_id], ObjectId):
            return doc[self.doc_id].generation_time           
        raise ValueError('Document does not have an ObjectId in the {} field'.format(self.doc_id))

    def get_earliest_date(self):
        return self._get_extremal_date(self._min_year, pymongo.ASCENDING)

    def get_latest_date(self):
        return self._get_extremal_date(self._max_year, pymongo.DESCENDING)

    def _doc_exists_and_id(self, doc):
        hsh = self._get_or_generate_hash(doc)
        results = [x for year in range(self._max_year, self._min_year-1, pymongo.DESCENDING) for x in self._collections[year].find({self._blackboard.counter_manager.get_hash_field() : hsh})]
        return (True, results[0][self.doc_id]) if results else (False, None)

    def _get_or_generate_id(self, doc):
        if self.doc_id not in doc:
            return ObjectId.from_datetime(datetime.now())
        return doc[self.doc_id]

    def _get_extremal_date(self, year, order):
        return self.get_date(self._collections[year].find().sort(self.doc_id, order).limit(1)[0])

    def _get_doc_year(self, doc):
        return self.get_date(doc).year

    def _parse_year_range(self, **kwargs):
        date = "{}-01-01"
        min_date = kwargs.get('min_date', [date.format(self._min_year)])
        max_date = kwargs.get('max_date', [date.format(self._max_year)])
        return (dtparser.parse(min_date[0]).year, dtparser.parse(max_date[0]).year)
