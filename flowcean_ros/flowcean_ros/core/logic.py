import importlib
from typing import Any
from dataclasses import fields, is_dataclass
from rosidl_runtime_py.utilities import get_message
import inspect
import collections.abc
import numpy as np

def set_field_value_of_ros_msg(msg_instance : Any, field : str, value : Any):
    fields = field.split(".")
    target = msg_instance
    
    # Walk down the attributes except the last one
    for f in fields[:-1]:
        target = getattr(target, f)

    # Set the final field
    setattr(target, fields[-1], value)


def get_msg_class(topic_name : str, type : list[str]):
    if len(type) > 1:
        raise RuntimeError(
            f"Multiple message types for topic {topic_name}: {type}",
        )
    # TODO: TEST IF I CAN USE THIS INSTEAD OF CODE BELOW   msg_class = get_message(type_name)
    
    module, _msg, cls = type[0].rsplit("/", 2)
    msg_class = getattr(
        importlib.import_module(module + "." + _msg),
        cls,
    )
    return msg_class


def get_all_fields_of_class(msg_class: Any, prefix="") -> list[str]:
    all_fields = []
    for field in msg_class.get_fields_and_field_types().keys():
        field_value = getattr(msg_class, field)
        full_name = f"{prefix}.{field}" if prefix else field
        
        # Is field a ROS message class?
        if hasattr(field_value, "__slots__"):
            # Recursively process subfields
            all_fields.extend(get_all_fields_of_class(field_value, full_name))
        else:
            # Base type, just add the full name
            all_fields.append(full_name)
    return all_fields

def msg_has_field(msg_class: Any, field: str) -> bool:
    """
    Recursively checks if a field is part of a msg.
    """
    if "." in field:
        field, nested_fields = field.split(".", 1)
    else:
        nested_fields = None

    field_types = msg_class.get_fields_and_field_types()
    if field not in field_types:
        return False

    if nested_fields is None:
        return True

    nested_msg_class = get_message(
        field_types[field],
    )
    if nested_msg_class is None:
        return False
    return msg_has_field(nested_msg_class, nested_fields)


def _unpack_to_dict(obj: object) -> object:
    if isinstance(obj, (tuple, list)):
        return type(obj)(_unpack_to_dict(item) for item in obj)
    if is_dataclass(obj) and not isinstance(obj, type):
        return {
            f.name: _unpack_to_dict(getattr(obj, f.name))
            for f in fields(obj)
            if f.name != "__msgtype__"
        }
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj



def helper_function(func):
    """
    this function clarifies that defined helper functions need *args
    """
    def validate_helper(func):
        sig = inspect.signature(func)

        # must have only one VAR_POSITIONAL parameter (*args)
        params = list(sig.parameters.values())
        if not (len(params) == 1 and params[0].kind == inspect.Parameter.VAR_POSITIONAL):
            raise TypeError(
                f"Helper '{func.__name__}' must have a *args signature, "
                f"but got {sig}"
            )
        return func


    func = validate_helper(func)

    def wrapper(*args):
        result = func(*args)
        if not isinstance(result, (list, tuple)):
            raise TypeError(
                f"Helper '{func.__name__}' must return a list/tuple"
            )
        return result
    return wrapper