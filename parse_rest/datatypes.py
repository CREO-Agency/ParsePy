#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import base64
import datetime

from connection import API_ROOT, ParseBase
import query 


class ParseType(object):

    @staticmethod
    def convert_from_parse(parse_data):
        is_parse_type = isinstance(parse_data, dict) and '__type' in parse_data
        if not is_parse_type:
            return parse_data

        parse_type = parse_data['__type']
        native = {
            'Pointer': Pointer,
            'Date': Date,
            'Bytes': Binary,
            'GeoPoint': GeoPoint,
            'File': File,
            'Relation': Relation
            }.get(parse_type)

        return native and native.from_native(**parse_data) or parse_data

    @staticmethod
    def convert_to_parse(python_object, as_pointer=False):
        is_object = isinstance(python_object, Object)

        if is_object and not as_pointer:
            return dict([(k, ParseType.convert_to_parse(v, as_pointer=True))
                         for k, v in python_object._editable_attrs.items()
                         ])


        python_type = type(python_object)
        if is_object:
            python_type = Object
        elif isinstance(python_object, ParseResource):
            python_type = ParseResource

        # classes that need to be cast to a different type before serialization
        transformation_map = {
            datetime.datetime: Date,
            Object: Pointer,
            ParseResource: Pointer,
        }


        if python_type in transformation_map:
            klass = transformation_map.get(python_type)
            return klass(python_object)._to_native()

        if isinstance(python_object, ParseType):
            return python_object._to_native()

        return python_object

    @classmethod
    def from_native(cls, **kw):
        return cls(**kw)

    def _to_native(self):
        return self._value


class Pointer(ParseType):

    @classmethod
    def from_native(cls, **kw):
        klass = Object.factory(kw.get('className'))
        return klass.retrieve(kw.get('objectId'))

    def __init__(self, obj):
        self._object = obj

    def _to_native(self):
        class_name = self._object.__class__.__name__
        return {
            '__type': 'Pointer',
            'className': class_name if class_name != "User" else "_User",
            'objectId': self._object.objectId
            }


class Relation(ParseType):
    @classmethod
    def from_native(cls, **kw):
        pass


class Date(ParseType):
    FORMAT = '%Y-%m-%dT%H:%M:%S.%f%Z'

    @classmethod
    def from_native(cls, **kw):
        return cls._from_str(kw.get('iso', ''))

    @staticmethod
    def _from_str(date_str):
        """turn a ISO 8601 string into a datetime object"""
        return datetime.datetime.strptime(date_str[:-1] + 'UTC', Date.FORMAT)

    def __init__(self, date):
        """Can be initialized either with a string or a datetime"""
        if isinstance(date, datetime.datetime):
            self._date = date
        elif isinstance(date, unicode):
            self._date = Date._from_str(date)

    def _to_native(self):
        return {
            '__type': 'Date', 'iso': self._date.isoformat()
            }


class Binary(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(kw.get('base64', ''))

    def __init__(self, encoded_string):
        self._encoded = encoded_string
        self._decoded = str(base64.b64decode(self._encoded))

    def _to_native(self):
        return {'__type': 'Bytes', 'base64': self._encoded}


class GeoPoint(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(kw.get('latitude'), kw.get('longitude'))

    def __init__(self, latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

    def _to_native(self):
        return {
            '__type': 'GeoPoint',
            'latitude': self.latitude,
            'longitude': self.longitude
            }


class File(ParseType):

    @classmethod
    def from_native(cls, **kw):
        return cls(**kw)

    def __init__(self, **kw):
        name = kw.get('name')
        self._name = name
        self._api_url = '/'.join([API_ROOT, 'files', name])
        self._file_url = kw.get('url')

    def _to_native(self):
        return {
            '__type': 'File',
            'name': self._name
            }

    url = property(lambda self: self._file_url)
    name = property(lambda self: self._name)
    _absolute_url = property(lambda self: self._api_url)


class ParseM2M(Relation):
    def convert_from_parse(parse_data):
        pass

    def convert_to_parse(python_object, as_pointer=False):
        pass


class Function(ParseBase):
    ENDPOINT_ROOT = '/'.join((API_ROOT, 'functions'))

    def __init__(self, name):
        self.name = name

    def __call__(self, **kwargs):
        return self.POST('/' + self.name, **kwargs)


class ParseResource(ParseBase, Pointer):

    PROTECTED_ATTRIBUTES = ['objectId', 'createdAt', 'updatedAt']

    def __eq__(self, other):
        if not isinstance(other, ParseResource):
            return False
        if not self.__class__ == other.__class__:
            return False

        if self.objectId is None:
            return self is other
        return self.objectId == other.objectId

    @classmethod
    def retrieve(cls, resource_id):
        return cls(**cls.GET('/' + resource_id))

    @property
    def _editable_attrs(self):
        protected_attrs = self.__class__.PROTECTED_ATTRIBUTES
        allowed = lambda a: a not in protected_attrs and not a.startswith('_')
        return dict([(k, v) for k, v in self.__dict__.items() if allowed(k)])

    def __init__(self, **kw):
        for key, value in kw.items():
            setattr(self, key, ParseType.convert_from_parse(value))

    def _to_native(self):
        return ParseType.convert_to_parse(self)

    def _get_object_id(self):
        return self.__dict__.get('_object_id')

    def _set_object_id(self, value):
        if '_object_id' in self.__dict__:
            raise ValueError('Can not re-set object id')
        self._object_id = value

    def _get_updated_datetime(self):
        return self.__dict__.get('_updated_at') and self._updated_at._date

    def _set_updated_datetime(self, value):
        self._updated_at = Date(value)

    def _get_created_datetime(self):
        return self.__dict__.get('_created_at') and self._created_at._date

    def _set_created_datetime(self, value):
        self._created_at = Date(value)

    def save(self, batch=False, **kwargs):
        if self.objectId:
            return self._update(batch=batch)
        else:
            return self._create(batch=batch)

    def _create(self, batch=False):
        uri = self.__class__.ENDPOINT_ROOT
        response = self.__class__.POST(uri, batch=batch, **self._to_native())

        def call_back(response_dict):
            self.createdAt = self.updatedAt = response_dict['createdAt']
            self.objectId = response_dict['objectId']

        if batch:
            return response, call_back
        else:
            call_back(response)

    def _update(self, batch=False):
        response = self.__class__.PUT(self._absolute_url, batch=batch,
                                      **self._to_native())

        def call_back(response_dict):
            self.updatedAt = response_dict['updatedAt']

        if batch:
            return response, call_back
        else:
            call_back(response)

    def delete(self, batch=False):
        response = self.__class__.DELETE(self._absolute_url, batch=batch)
        def call_back(response_dict):
            self.__dict__ = {}

        if batch:
            return response, call_back
        else:
            call_back(response)

    _absolute_url = property(
        lambda self: '/'.join([self.__class__.ENDPOINT_ROOT, self.objectId])
        )

    objectId = property(_get_object_id, _set_object_id)
    createdAt = property(_get_created_datetime, _set_created_datetime)
    updatedAt = property(_get_updated_datetime, _set_updated_datetime)

    def __repr__(self):
        return '<%s:%s>' % (unicode(self.__class__.__name__), self.objectId)


class ParseField(object):
    _default = None

    def _get_default(self):
        return self._default

    def _set_default(self, value):
        self._default = value
    default = property(_get_default, _set_default)

    def __init__(self, *args, **kwargs):
        self._update_attrs(kwargs)

    def _update_attrs(self, dct):
        for key, value in dct.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError('%s does not support attribute %s' % ( 
                    self.__class__,
                    key,
                ))  


class ParseManyToManyField(ParseField):
    _model_class = None
    _related_class = None
    
    def __init__(self, related, *args, **kwargs):
        self._related_class = related
        super(ParseManyToManyField, self).__init__(*args, **kwargs)

    def query_manager(self, instance):
        return query.M2MQueryManager(
            self._model_class, self._related_class, instance)

    def add_to_class(self, cls):
        self._model_class = cls


class ObjectMetaclass(type):
    def __new__(cls, name, bases, dct):
        cls = super(ObjectMetaclass, cls).__new__(cls, name, bases, dct)
        cls.set_endpoint_root()
        cls.Query = query.QueryManager(cls)
        try:
            module = dct.pop('__module__')
        except KeyError:
            pass
        fields = cls._get_fields(dct)
        cls._defaults = cls._get_defaults(dct)
        cls._m2m_fields = cls._get_m2m_fields(dct)
        for attr, field in cls._get_m2m_fields(dct).items():
            def m2mgetter(self):
                return field.query_manager(self)

            def m2msetter(self, val):
                field.query_manager(self).clear()
                field.query_manager(self).add(*val)

            field.add_to_class(cls)
            setattr(cls, attr, property(m2mgetter, m2msetter))
            del cls._defaults[attr]
        return cls 

    def _get_fields(cls, dct):
        return dict(
            [   
                (field, value)
                for field, value 
                in dct.items() 
                if isinstance(value, ParseField)
            ]   
        )   

    def _get_m2m_fields(cls, dct):
        return dict(
            [
                (field, value)
                for field, value
                in dct.items()
                if isinstance(value, ParseManyToManyField)
            ]
        )

    def _get_defaults(cls, dct):
        return dict([
            (field, value.default)
            for field, value
            in dct.items()
            if isinstance(value, ParseField)
        ])  


class Object(ParseResource):
    __metaclass__ = ObjectMetaclass
    ENDPOINT_ROOT = '/'.join([API_ROOT, 'classes'])

    def __init__(self, **kwargs):
        for key, value in self._defaults.items():
            if callable(value):
                value = value()
            setattr(self, key, ParseType.convert_from_parse(value))

        super(Object, self).__init__(**kwargs)

    @classmethod
    def factory(cls, class_name):
        class DerivedClass(cls):
            pass
        if class_name == "_User":
            from user import User
            return User
        DerivedClass.__name__ = str(class_name)
        DerivedClass.set_endpoint_root()
        return DerivedClass

    @classmethod
    def defaults(cls):
        return cls._defaults

    @classmethod
    def set_endpoint_root(cls):
        root = '/'.join([API_ROOT, 'classes', cls.__name__])
        if cls.ENDPOINT_ROOT != root:
            cls.ENDPOINT_ROOT = root
        return cls.ENDPOINT_ROOT

    @property
    def _absolute_url(self):
        if not self.objectId: return None
        return '/'.join([self.__class__.ENDPOINT_ROOT, self.objectId])

    @property
    def as_pointer(self):
        return Pointer(**{
                'className': self.__class__.__name__,
                'objectId': self.objectId
                })

    def increment(self, key, amount=1):
        """
        Increment one value in the object. Note that this happens immediately:
        it does not wait for save() to be called
        """
        payload = {
            key: {
                '__op': 'Increment',
                'amount': amount
                }
            }
        self.__class__.PUT(self._absolute_url, **payload)
        self.__dict__[key] += amount
