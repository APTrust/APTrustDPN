"""
    I just wanted to say that I'm a nerd, and I'm here 
    tonight to stand up for the rights of other nerds. 
    
            - Gibert

"""

class MsgTracker(object):
    """
    Due to the iterative nature of the MessageSchema, this class was 
    introduced in order to keep track of the original data being
    validated by a MessageSchema for later usage in error debugging.
    """
    
    _original_data = None
    
    @classmethod
    def set_data(cls,data):
        """
        Save the original data being validated by MessageSchema in 
        _original_data variable.
        
        :param data: original data being validated by Message Schema.
        :return: Original MessageSchema data object.
        """
		
        cls._original_data = data
        return cls._original_data
        
    @classmethod
    def data(cls):
        """
        Return from _original_data variable the original data being 
        validated by MessageSchema.
		
        :return: Original MessageSchema data object.
        """
		
        return cls._original_data
  

class MessageSchema(object):

    def __init__(self, schema, error=None, sub=False):
        """
        A simple object-schema based validator in order to be used for 
        Message validation.

        :param schema: object containing structure and validations to be verified by the MessageSchema instance.
        :param error: Custom error message to append to any validation error raised by MessageSchema.
        :param sub: Defaults to false. Set to true only within MessageSchema iterations to make sure MsgTracker class data does not get overwritten.
        :return: None
        """
		
        self._schema = schema
        self._error = error
        self._sub = sub

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._schema)

    def validate(self, data):
        s = self._schema
        e = self._error
        if self._sub == False:
          MsgTracker.set_data(data)
        if type(s) in (list, tuple, set, frozenset):
            data = MessageSchema(type(s), error=e, sub=True).validate(data)
            return type(s)(Or(*s, error=e).validate(d) for d in data)
        if type(s) is dict:
            data = MessageSchema(dict, error=e, sub=True).validate(data)
            new = type(data)()  # new - is a dict of the validated values
            x = None
            coverage = set()  # non-optional schema keys that were matched
            # for each key and value find a schema entry matching them, if any
            sorted_skeys = list(sorted(s, key=priority))
            for key, value in data.items():
                valid = False
                skey = None
                for skey in sorted_skeys:
                    svalue = s[skey]
                    try:
                        nkey = MessageSchema(skey, error=e, sub=True).validate(key)
                    except MessageSchemaError:
                        pass
                    else:
                        try:
                            nvalue = MessageSchema(svalue, error=e, sub=True).validate(value)
                        except MessageSchemaError as _x:
                            x = _x
                            raise
                        else:
                            coverage.add(skey)
                            valid = True
                            break
                if valid:
                    new[nkey] = nvalue
                elif skey is not None:
                    if x is not None:
                        raise MessageSchemaError(['invalid value for key %r' % key] +
                                          x.autos, [e] + x.errors)
            coverage = set(k for k in coverage if type(k) is not Optional)
            required = set(k for k in s if type(k) is not Optional)
            if coverage != required:
                raise MessageSchemaError('missed keys %r in: %r' % (required - coverage, MsgTracker.data()), e)
            if len(new) != len(data):
                wrong_keys = set(data.keys()) - set(new.keys())
                s_wrong_keys = ', '.join('%r' % k for k in sorted(wrong_keys))
                raise MessageSchemaError('wrong keys %s in %r' % (s_wrong_keys, data),
                                  e)
            return new
        if hasattr(s, 'validate'):
            try:
                return s.validate(data)
            except MessageSchemaError as x:
                raise MessageSchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise MessageSchemaError('%r.validate(%r) raised %r in: %r' % (s, data, x, MsgTracker.data()),
                                  self._error)
        if type(s) is type:
            if isinstance(data, s):
                return data
            else:
                raise MessageSchemaError('%r should be instance of %r in: %r' % (data, s, MsgTracker.data()), e)
        if callable(s):
            f = s.__name__
            try:
                if s(data):
                    return data
            except MessageSchemaError as x:
                raise MessageSchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise MessageSchemaError('%s(%r) raised %r' % (f, data, x),
                                  self._error)
            raise MessageSchemaError('%s(%r) should evaluate to True in: %r' % (f, data, MsgTracker.data()), e)
        if s == data:
            return data
        else:
            raise MessageSchemaError('%r does not match %r' % (s, data), e)


class MessageSchemaError(Exception):
    """Error during MessageSchema validation."""

    def __init__(self, autos, errors=[]):
        self.autos = autos if type(autos) is list else [autos]
        self.errors = errors if type(errors) is list else [errors]
        Exception.__init__(self, self.code)

    @property
    def code(self):
        def uniq(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if x not in seen and not seen_add(x)]
        a = uniq(i for i in self.autos if i is not None)
        e = uniq(i for i in self.errors if i is not None)
        if e:
            return '\n'.join(e)
        return '\n'.join(a)


class And(object):

    def __init__(self, *args, **kw):
        self._args = args
        assert list(kw) in (['error'], [])
        self._error = kw.get('error')

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(repr(a) for a in self._args))

    def validate(self, data):
        for s in [MessageSchema(s, error=self._error, sub=True) for s in self._args]:
            data = s.validate(data)
        return data


class Or(And):

    def validate(self, data):
        x = MessageSchemaError([], [])
        for s in [MessageSchema(s, error=self._error, sub=True) for s in self._args]:
            try:
                return s.validate(data)
            except MessageSchemaError as _x:
                x = _x
        raise MessageSchemaError(['%r did not validate %r' % (self, data)] + x.autos,
                          [self._error] + x.errors)


class Use(object):

    def __init__(self, callable_, error=None):
        assert callable(callable_)
        self._callable = callable_
        self._error = error

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._callable)

    def validate(self, data):
        try:
            return self._callable(data)
        except MessageSchemaError as x:
            raise MessageSchemaError([None] + x.autos, [self._error] + x.errors)
        except BaseException as x:
            f = self._callable.__name__
            raise MessageSchemaError('%s(%r) raised %r' % (f, data, x), self._error)


def priority(s):
    """Return priority for a give object."""
    if type(s) in (list, tuple, set, frozenset):
        return 6
    if type(s) is dict:
        return 5
    if hasattr(s, 'validate'):
        return 4
    if type(s) is type:
        return 3
    if callable(s):
        return 2
    else:
        return 1


class Optional(MessageSchema):
    """Marker for an optional part of MessageSchema."""