from collections import OrderedDict
from copy import deepcopy

"""tools to make open-data formatting easier. it works with the `stappy` library."""

VERSION_STR = '0.1.0'

METADATA_ENTRY = 'metadata'
SUBJECT_KEY    = 'subject'
DATE_KEY       = 'date'
SESSION_KEY    = 'session_number'
DOMAIN_KEY     = 'domain'

DEFINITION_KEY = 'definition'
VALUE_KEY      = 'value'
UNIT_KEY       = 'unit'

### Ontological structure

def is_root(entry):
    return not within_subject(entry)

def within_subject(entry):
    return SUBJECT_KEY in entry.attrs[METADATA_ENTRY].keys()

def is_subject(entry):
    return within_subject(entry) and not within_date(entry)

def within_date(entry):
    return DATE_KEY in entry.attrs[METADATA_ENTRY].keys()

def is_date(entry):
    return within_date(entry) and not within_session(entry)

def within_session(entry):
    return SESSION_KEY in entry.attrs[METADATA_ENTRY].keys()

def is_session(entry):
    return within_session(entry) and not is_group(entry)

def is_group(entry):
    return DOMAIN_KEY in entry.attrs[METADATA_ENTRY].keys()

### reading

def iter_subjects(entry):
    if not is_root(entry):
        raise ValueError("iter_subjects() must be called with the root entry")
    for sub_name in entry.child_names():
        yield sub_name, entry[sub_name]

def iter_dates(entry):
    if is_root(entry):
        for sub_name, subject in iter_subjects(entry):
            for date_value in subject.child_names():
                yield (sub_name, date_value), subject[date_value]
    elif is_subject(entry):
        for date_value in entry.child_names():
            yield date_value, entry[date_value]
    else:
        raise ValueError("iter_dates() must be called with a root or subject entry.")

def iter_sessions(entry):
    if within_date(entry):
        if within_session(entry):
            raise ValueError("iter_sessions() must be called with a root/subject/date entry.")
        else:
            # date entry
            for session_number in entry.child_names():
                yield session_number, entry[session_number]
    else:
        for path, date in iter_dates(entry):
            if not isinstance(path, tuple):
                path = (path,)
            for session_number in date.child_names():
                yield (path + (session_number,)), date[session_number]

### manipulation

class Attribute:
    """a tool for making attribute definitions easier"""
    def __init__(self, basedict=None, definition=''):
        if basedict is not None:
            self._content = OrderedDict(basedict)
        else:
            self._content = OrderedDict()
        self._content[DEFINITION_KEY] = definition

    def add_group(self, name, definition=''):
        """adds and returns a new child attribute group"""
        self._content[name] = Attribute(definition=definition)
        return self._content[name]

    def add_value(self, name, value, definition='', unit=''):
        """adds a value (primitive, dict or list) to this attribute, and returns it."""
        self._content[name] = OrderedDict()
        self._content[name][DEFINITION_KEY] = definition
        self._content[name][VALUE_KEY]      = value
        self._content[name][UNIT_KEY]       = unit
        return self._content[name]

    def as_dict(self):
        """formats itself as an ordered dictionary"""
        ret = OrderedDict()
        for key, struct in self._content.items():
            if isinstance(struct, Attribute):
                struct = struct.as_dict()
            ret[key] = struct
        return ret

    def add_to_parent(self, parent, path):
        """adds itself to the parent's attribute at `path`"""
        parent.attrs[path] = self.as_dict()

def copy_attributes(src, dst):
    for key, val in src.attrs.items():
        dst.attrs[key] = deepcopy(val)
    dst.attrs.commit()

def add_attribute(parent, path, value, definition='', unit=None):
    parent.attrs[f'{path}/{DEFINITION_KEY}'] = definition
    parent.attrs[f'{path}/{VALUE_KEY}']      = value
    if unit is not None:
        parent.attrs[f'{path}/{UNIT_KEY}']   = unit

def set_description(root_entry, desc=''):
    """returns the root entry."""
    root_entry.attrs[f'{METADATA_ENTRY}/description'] = desc
    root_entry.attrs.commit()
    return root_entry

def add_subject(root_entry, name):
    """returns the subject entry."""
    if name is None:
        raise ValueError("name cannot be None")
    if not is_root(root_entry):
        raise ValueError("must be called with root entry")
    subject_entry = root_entry.create[name]
    copy_attributes(root_entry, subject_entry)
    subject_entry.attrs[f'{METADATA_ENTRY}/{SUBJECT_KEY}'] = name
    return subject_entry

def add_date(subject_entry, date):
    """returns the date entry."""
    if date is None:
        raise ValueError("date cannot be None")
    if not is_subject(subject_entry):
        raise ValueError("must be called with subject entry")
    date_entry = subject_entry.create[date]
    copy_attributes(subject_entry, date_entry)
    date_entry.attrs[f'{METADATA_ENTRY}/{DATE_KEY}'] = date
    return date_entry

def add_session(date_entry, number):
    """returns the session entry."""
    if number is None:
        raise ValueError("number cannot be None")
    if not is_date(date_entry):
        raise ValueError("must be called with date entry")
    try:
        number = int(number)
    except ValueError:
        raise ValueError("number cannot be parsed as integer")
    session_entry = date_entry.create[str(number)]
    copy_attributes(date_entry, session_entry)
    session_entry.attrs[f'{METADATA_ENTRY}/{SESSION_KEY}'] = number
    return session_entry

def add_group(parent, name, definition=''):
    """returns the created group."""
    if name is None:
        raise ValueError("name cannot be None")
    if not within_session(parent):
        raise ValueError("must be called with a session or group entry")
    group = parent.create[name]
    group.attrs[METADATA_ENTRY] = deepcopy(parent.attrs[METADATA_ENTRY])
    group.attrs[f'{METADATA_ENTRY}/{DOMAIN_KEY}'] = definition
    parent.attrs[f'{name}/{DEFINITION_KEY}'] = definition
    return group

def add_filepath(parent, filename, definition=''):
    """returns the path to filename."""
    if filename is None:
        raise ValueError("filename cannot be None")
    # FIXME implementation specifics! only works with FileSystemDatabase
    parent_path = parent._repr
    file_path   = parent_path / filename
    parent.attrs[f'{file_path.name}/{DEFINITION_KEY}'] = definition
    return file_path

def add_dataset(parent, name, value, definition='', unit=''):
    """returns None.
    `parent` may be either session or group entry."""
    if name is None:
        raise ValueError("name cannot be None")
    parent[name] = value
    parent.attrs[f'{name}/{DEFINITION_KEY}'] = definition
    parent.attrs[f'{name}/{UNIT_KEY}']       = unit
    parent.attrs.commit()
