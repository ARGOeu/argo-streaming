#!/usr/bin/env python
from ConfigParser import SafeConfigParser
from urlparse import urlparse
import json
import re
from os import path


class Template:
    """
    Implements option parameters that are templates
    """

    def __init__(self, tmpl, sub_type):
        self.tmpl = tmpl
        self.sub_type = sub_type

    def __repr__(self):
        """
        String representation of template object
        """
        return self.tmpl

    def get_args(self):

        """
        Get arguments used in template

        Returns:
            list: a list of arguments used in the tamplate
        """
        return re.findall(r"{{\s*(.*?)\s*}}", self.tmpl)

    def fill(self, **args_new):
        """
         Fill template with argument values and get result

         Args:
             args_new = list of argument key,value pairs fill the template
         Returns:
             obj: Fills template and returns an appropriate object based on template type
         """
        txt = self.tmpl  # type: str
        args = self.get_args()
        if sorted(args) != sorted(args_new.keys()):
            raise RuntimeError("Argument mismatch, needed arguments:"+str(args))
        for arg in args:
            txt = re.sub(r"{{\s*"+str(arg)+r"\s*}}", str(args_new[arg]), txt)

        return self.get_as(txt)

    def get_as(self, text):
        """
        Get template result as a string and convert it to an appropriate return type

        Args:
            text: str. template result
        Returns:
            obj: template result in appropriate type
        """
        if self.sub_type == "string":
            return str(text)
        elif self.sub_type == "int" or self.sub_type == "long":
            return int(text)
        elif self.sub_type == "float":
            return float(text)
        elif self.sub_type == "uri":
            return urlparse(text)
        elif self.sub_type == "list":
            return text.split(",")
        elif self.sub_type == "path":
            return path.normpath(text)


class ArgoConfig:
    """
    ArgoConfig implements a class that parser argo fixed and dynamic configuration
    based on a specific schema
    """

    def __init__(self):
        self.conf = SafeConfigParser()
        self.schema = dict()
        self.fix = dict()
        self.var = dict()

    def get(self, group, item):
        """
        Given a group and an item return its value

        Args:
            group: str. group name
            item: str. item name
        Returns:
            obj: an object containing the value
        """

        if group in self.fix:
            if item in self.fix[group]:
                return self.fix[group][item]["value"]
        if group in self.var:
            if item in self.var[group]:
                return self.var[group][item]["value"]
        return None

    def load_conf(self, conf_path):
        """
        Load configuration from file using a SafeConfigParser
        """
        self.conf.read(conf_path)

    def load_schema(self, schema_path):
        """
        Load configuration schema (JSON format) from file
        """
        with open(schema_path, 'r') as schema_file:
            self.schema = json.load(schema_file)

    def get_as(self, group, item, item_type, og_item):
        """
        Return appropriate value dict object

        Args:
            group: str. group name
            item: str. item name
            item_type: str. type of the item
            og_item: str. optional reference to original item in schema


        Returns:
            dict: result dictionary with value and optional reference to original item in schema
        """
        result = None
        if item_type == "string":
            result = self.conf.get(group, item)
        elif item_type == "int" or item_type == "long":
            result = self.conf.getint(group, item)
        elif item_type == "float":
            result = self.conf.getfloat(group, item)
        elif item_type == "uri":
            result = urlparse(self.conf.get(group, item))
        elif item_type == "list":
            result = self.conf.get(group, item).split(",")
        elif item_type == "path":
            result = path.normpath(self.conf.get(group, item))
        elif item_type.startswith("template"):
            tok = item_type.split(",")
            if len(tok) > 1:
                sub_type = tok[1]
            else:
                sub_type = "string"
            result = Template(self.conf.get(group, item), sub_type)

        pack = dict()
        pack["value"] = result

        if og_item != item:
            pack["og_item"] = og_item

        return pack

    def add_config_item(self, group, item, og_item, dest, og_group):
        """
        Add new item to the ArgoConfig params

        Args:
            group: str. group name
            item: str. item name
            og_item: str. reference to original item in schema
            dest: dict. where to add the item (in the fixed or varied item dictionary)
            og_group: str. reference to original group in schema
        """

        if group not in dest:
            dest[group] = dict()
            if og_group is not None:
                dest[group]["og_group"] = og_group
        if og_group is not None:
            dest[group][item] = self.get_as(group,  item, self.schema[og_group][og_item]["type"], og_item)
        else:
            dest[group][item] = self.get_as(group, item, self.schema[group][og_item]["type"], og_item)

    def add_group_items(self, group, items, var, og_group):
        """
        Add a list of items to a group. If var=true the items are treated as
        varied items.

        Args:
            group: str. group name
            items: list(str). list of item names
            var: bool. if the items are considered varied
            og_group: str. reference to original group in schema
        """
        if var:
            dest = self.var
        else:
            dest = self.fix

        for item in items:
            if type(item) is not dict:
                self.add_config_item(group, item, item, dest, og_group)
            else:
                for sub_item in item["vars"]:
                    self.add_config_item(group, sub_item, item["item"], dest, og_group)

    @staticmethod
    def is_var(name):
        """
        If a name is considered to represent a varied object"

        Returns:
            bool. is considered varied or not
        """
        if "~" in name:
            return True

        return False

    def get_item_variations(self, group, item, ogroup):
        """
        Search schema for the field that provides the variations
        and create a list of the expected varied items

        Returns:
            list(str). a list with all available item name variations
        """
        variations = {'group': group, 'item': item, 'vars': list()}

        if ogroup is not None:
            map_pool = self.schema[ogroup][item]["~"]
        else:
            map_pool = self.schema[group][item]["~"]
        if '.' in map_pool:
            map_pool = map_pool.split(".")
        else:
            tmp_item = map_pool
            map_pool = list()
            map_pool.append(group)
            map_pool.append(tmp_item)

        name_pool = self.conf.get(map_pool[0], map_pool[1]).split(",")

        for name in name_pool:
            variations["vars"].append(item.replace("~", name))
        return variations

    def get_group_variations(self, group):
        """
        Search schema for the field that provides the variations
        and create a list of the expected varied groups

        Returns:
            list(str). a list with all available group name variations
        """
        variations = {'group': group, 'vars': list()}

        map_pool = self.schema[group]["~"]
        if '.' in map_pool:
            map_pool = map_pool.split(".")
        else:
            item = map_pool
            map_pool = list()
            map_pool.append(group)
            map_pool.append(item)

        name_pool = self.conf.get(map_pool[0], map_pool[1]).split(",")
        for name in name_pool:
            variations["vars"].append(group.replace("~", name))
        return variations

    def check_conf(self):

        """
        Validate schema and configuration file. Iterate and extract
        all configuration parameters
        """
        fix_groups = self.schema.keys()
        var_groups = list()

        for group in fix_groups:
            if self.is_var(group):
                var_groups.append(self.get_group_variations(group))
                continue

            fix_items = list()
            var_items = list()
            for item in self.schema[group].keys():
                if self.is_var(item):
                    var_items.append(self.get_item_variations(group, item, None))
                    continue
                fix_items.append(item)
            self.add_group_items(group, fix_items, False, None)
            self.add_group_items(group, var_items, True, None)

        for group in var_groups:
            for sub_group in group["vars"]:
                fix_items = list()
                var_items = list()
                for item in self.schema[group["group"]].keys():

                    if item == "~":
                        continue

                    if self.is_var(item):
                        var_items.append(self.get_item_variations(sub_group, item, group["group"]))
                        continue
                    fix_items.append(item)
                # Both fix and var items are in a var group so are considered var
                self.add_group_items(sub_group, fix_items, True, group["group"])
                self.add_group_items(sub_group, var_items, True, group["group"])


def main():
    # Local files
    conf_fn = "argo-streaming.conf"
    schema_fn = "config.schema.json"

    argo_conf = ArgoConfig()
    argo_conf.load_conf(conf_fn)
    argo_conf.load_schema(schema_fn)
    argo_conf.check_conf()

    print "Fixed parameters:"
    print json.dumps(argo_conf.fix, default=str, indent=2)
    print "Variable parameters:"
    print json.dumps(argo_conf.var, default=str, indent=2)


if __name__ == '__main__':
    main()
