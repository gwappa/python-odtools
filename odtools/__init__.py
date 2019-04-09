import json
from collections import OrderedDict
from copy import deepcopy
import shutil

"""tools to make open-data formatting easier. it works with the `stappy` library."""

VERSION_STR = '0.2.0'

METADATA_ENTRY = 'metadata'
SUBJECT_KEY    = 'subject'
DATE_KEY       = 'date'
SESSION_KEY    = 'session_number'
DOMAIN_KEY     = 'domain'
RUN_KEY        = 'run'

TYPE_KEY       = 'type'
DEFINITION_KEY = 'definition'
VALUE_KEY      = 'value'
UNIT_KEY       = 'unit'

DATASET_TYPE   = 'dataset'
FILE_TYPE      = 'file'

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
    return within_domain(entry) or is_run(entry)

def within_domain(entry):
    return DOMAIN_KEY in entry.attrs[METADATA_ENTRY].keys()

def is_domain(entry):
    return within_domain(entry) and not is_run(entry)

def is_run(entry):
    return RUN_KEY in entry.attrs[METADATA_ENTRY].keys()

def is_formatting_attribute(attrname):
    # FIXME: implementation specific
    return attrname in ('type', 'dtype','shape','compression','byteorder','definition','unit')

def is_subentry(entry, name):
    return name in entry.child_names()

def is_dataset_type(entry, name):
    return name in entry.dataset_names()

def is_file_type(entry, name):
    return (TYPE_KEY in entry.attrs[name].keys()) and (entry.attrs[name][TYPE_KEY] == FILE_TYPE)

def is_attribute(entry, name):
    return (not is_dataset_type(entry, name)) and (not is_file_type(entry, name))

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

def get_filepath(parent, filename):
    if filename is None:
        raise ValueError("filename cannot be None")
    parent_path = parent._repr
    file_path   = parent_path / filename
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    return file_path

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
        if definition is not None:
            self._content[name][DEFINITION_KEY] = definition
        self._content[name][VALUE_KEY]      = value
        if unit is not None:
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

def add_group(parent, name, key=None, definition=''):
    """returns the created group."""
    if name is None:
        raise ValueError("name cannot be None")
    if key is None:
        raise ValueError("key cannot be None for a group")
    # if not within_session(parent):
    #     raise ValueError("must be called with a session or domain entry")
    group = parent.create[name]
    if METADATA_ENTRY in parent.attrs.keys():
        group.attrs[METADATA_ENTRY] = deepcopy(parent.attrs[METADATA_ENTRY])

    group.attrs[f'{METADATA_ENTRY}/{key}'] = definition
    parent.attrs[f'{name}/{DEFINITION_KEY}']      = definition
    return group

def add_domain(parent, name, definition=''):
    """returns the created domain."""
    return add_group(parent, name, key=DOMAIN_KEY, definition=definition)

def add_run(parent, name, definition=''):
    """returns the created run."""
    # if not (is_session(parent) or is_domain(parent)):
    #     raise ValueError("a run must be under a session or a domain.")
    return add_group(parent, name, key=RUN_KEY, definition=definition)

def add_filepath(parent, filename, definition=''):
    """returns the path to filename under `parent`."""
    if filename is None:
        raise ValueError("filename cannot be None")
    # FIXME implementation specifics! only works with FileSystemDatabase
    parent_path = parent._repr
    file_path   = parent_path / filename
    parent.attrs[f'{file_path.name}/{DEFINITION_KEY}'] = definition
    parent.attrs[f'{file_path.name}/{TYPE_KEY}']       = FILE_TYPE
    return file_path

def add_dataset(parent, name, value, definition='', unit=''):
    """returns None.
    `parent` may be either session or domain entry
    (but add_dataset() does not throw an error even when it is not)."""
    if name is None:
        raise ValueError("name cannot be None")
    parent[name] = value
    parent.attrs[f'{name}/{DEFINITION_KEY}'] = definition
    parent.attrs[f'{name}/{UNIT_KEY}']       = unit
    parent.attrs[f'{name}/{TYPE_KEY}']       = DATASET_TYPE
    parent.attrs.commit()

def copy_dataset(source, dest, name, destname=None):
    if destname is None:
        destname = name
    add_dataset(dest, destname, source[name],
            definition=source.attrs[f"{name}/{DEFINITION_KEY}"],
            unit=source.attrs[f"{name}/{UNIT_KEY}"])
    for attrname in source.attrs[name].keys():
        if not is_formatting_attribute(attrname):
            dest.attrs[f"{destname}/{attrname}"] = deepcopy(source.attrs[f"{name}/{attrname}"])

def copy_file(source, dest, name, destname=None):
    if destname is None:
        destname = name
    # FIXME: implementation specific
    sourcepath = source._repr / name
    destpath   = dest.add_filepath(destname, definition=source.attrs[f"{name}/{DEFINITION_KEY}"])
    shutil.copy(sourcepath, destpath)
    for attrname in source.attrs[name].keys():
        if not is_formatting_attribute(attrname):
            dest.attrs[f"{destname}/{attrname}"] = deepcopy(source.attrs[f"{name}/{attrname}"])

def copy_children(source, dest):
    for name in source.attrs.keys():
        if is_dataset_type(source, name):
            copy_dataset(source, dest, name)
        elif is_file_type(source, name):
            copy_file(source, dest, name)
        elif name == METADATA_ENTRY:
            pass
        else:
            # assume it is attribute
            dest.attrs[name] = deepcopy(source.attrs[name])

# classes

class DataFormat:
    """data that can be stored through odtools.
    a subclass must provide stored `names`/`definitions`/`units`/`types`
    in the form of attributes, with their keys being stored attribute names."""

    _storage_funcs = {
        'dataset':   add_dataset,
        'attribute': add_attribute
    }

    def store_under(self, group, name, entry_names=None, entry_defs=None, entry_units=None):
        """store data under a stappy `group`, as another group containing datasets and attributes.

        default names/definitions/units can be updated by supplying
        updated values in the form of a dictionary as `names`,
        `definitions` or `units`."""

        names = self.names.copy()
        defs  = self.definitions.copy()
        units = self.units.copy()
        if entry_names is not None:
            names.update(entry_names)
        if entry_defs is not None:
            defs.update(entry_defs)
        if entry_units is not None:
            units.update(entry_units)

        entry = add_run(group, name, definition=defs.get('__self__', self.__class__.__name__))

        for attr in names.keys():
            name        = names[attr]
            value       = getattr(self, attr)
            definition  = defs[attr]
            unit        = units[attr]

            valuetype   = self.types[attr]
            if valuetype in self._storage_funcs.keys():
                self._storage_funcs[valuetype](entry, name, value,
                                    definition=definition, unit=unit)
            else:
                raise ValueError(f"value type not understood: {valuetype}")
        return entry

class KeyValueFormat:
    """for data classes that can be stored as key-value mappings.
    a subclass must provide as_dict(base=None) method, and `name` and `definition` attributes.

    currently only supports file system-type stappy databases."""

    def store_under(self, group, name=None, definition=None, add_metadata=True):
        name_used = name if name is not None else self.name
        def_used  = definition if definition is not None else self.definition

        if add_metadata == True:
            if METADATA_ENTRY in group.attrs.keys():
                meta = { METADATA_ENTRY: group.attrs[METADATA_ENTRY] }
            elif hasattr(self, 'metadata'):
                meta = { METADATA_ENTRY: self.metadata }
            else:
                meta = { METADATA_ENTRY: '(metadata not found)' }
            content = self.as_dict(base=meta)
        else:
            content = self.as_dict()

        filename  = name_used + '.json'
        path      = add_filepath(group, filename, definition=def_used)
        with open(path, 'w') as out:
            json.dump(content, out, indent=4)
        return path
