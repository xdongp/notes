# -*- coding: utf-8 -*-
from ctypes import *
import os
import struct

CRUSH_MAGIC = 0x00010000

# 考察了Python中的ENUM，和数组差别不大，所以ENUM采用数组实现
CRUSH_BUCKET = ['CRUSH_BUCKET_NULL',
                'CRUSH_BUCKET_UNIFORM',
                'CRUSH_BUCKET_LIST',
                'CRUSH_BUCKET_TREE',
                'CRUSH_BUCKET_STRAW']

CRUSH_ALG_NAME = ['null',
                  'uniform',
                  'list',
                  'tree',
                  'straw']

CRUSH_RULE = [
    'CRUSH_RULE_NOOP',
    'CRUSH_RULE_TAKE',  # /* arg1 = value to start with */
    'CRUSH_RULE_CHOOSE_FIRSTN',  # /* arg1 = num items to pick */
    # /* arg2 = type */
    'CRUSH_RULE_CHOOSE_INDEP',  # /* same */
    'CRUSH_RULE_EMIT',  # /* no args */
    'CRUSH_RULE_CHOOSE_LEAF_FIRSTN',
    'CRUSH_RULE_CHOOSE_LEAF_INDEP']

RULE_OP_CODE = ["noop",  # CRUSH_RULE_NOOP
                "take",  # CRUSH_RULE_TAKE
                "choose firstn",  # CRUSH_RULE_CHOOSE_FIRSTN
                "choose indep",  # CRUSH_RULE_CHOOSE_INDEP
                "emit",  # CRUSH_RULE_EMIT
                "noop",  # CRUSH_RULE_NOOP
                "chooseleaf firstn",  # CRUSH_RULE_CHOOSE_LEAF_FIRSTN
                "chooseleaf indep"  # CRUSH_RULE_CHOOSE_LEAF_INDEP
                ]

CEPF_PG_TYPE = ['null',
                'replicated',  # CEPH_PG_TYPE_REP = 1
                'raid4'  # CEPH_PG_TYPE_RAID4 = 2
                ]


class Error(Exception):
    def __init__(self, s):
        pass


def get_item_name(i, map):
    name = map.get(str(i), None)
    if not name:
        if i >= 0:
            name = "device%d" % i
        else:
            name = "bucket%d" % (-1 - i)
    return name


class Struct(Structure):
    def __str__(self):
        l = []
        for item in self._fields_:
            l.append("%s:%s" % (item[0], getattr(self, item[0])))
        for name, value in vars(self).items():
            l.append("%s:%s" % (name, value))
        return "\n".join(l)


class CrushRuleStep(Struct):
    CRUSH_RULE_NOOP = 0
    CRUSH_RULE_TAKE = 1  # /* arg1 = value to start with */
    CRUSH_RULE_CHOOSE_FIRSTN = 2  # /* arg1 = num items to pick  arg2 = type */
    CRUSH_RULE_CHOOSE_INDEP = 3  # /* same */
    CRUSH_RULE_EMIT = 4  # /* no args */
    CRUSH_RULE_CHOOSE_LEAF_FIRSTN = 6
    CRUSH_RULE_CHOOSE_LEAF_INDEP = 7

    _fields_ = [('op', c_uint32),
                ('arg1', c_int32),
                ('arg2', c_int32)]

    def dump(self, types_map, devices_map):
        if self.op in (CrushRuleStep.CRUSH_RULE_NOOP, CrushRuleStep.CRUSH_RULE_EMIT):
            print "\tstep %s" % RULE_OP_CODE[self.op]
        if self.op == CrushRuleStep.CRUSH_RULE_TAKE:
            print "\tstep %s %s" % (RULE_OP_CODE[self.op], get_item_name(self.arg1, devices_map))
        if self.op in (CrushRuleStep.CRUSH_RULE_CHOOSE_FIRSTN, CrushRuleStep.CRUSH_RULE_CHOOSE_INDEP,
                       CrushRuleStep.CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CrushRuleStep.CRUSH_RULE_CHOOSE_LEAF_INDEP):
            # print types_map
            type_name = types_map.get(str(self.arg2), "")
            print "\tstep %s %d type %s" % (RULE_OP_CODE[self.op], self.arg1, type_name)


class CrushRuleMask(Struct):
    _fields_ = [('ruleset', c_int8),
                ('type', c_int8),
                ('min_size', c_int8),
                ('max_size', c_int8)]


class CrushRule(Struct):
    _fields_ = [('len', c_uint32)]

    def __init__(self):
        self.mask = None
        self.steps = []

    def get_ruleset(self):
        return self.mask & 0x000000ff

    def get_type(self):
        return (self.mask & 0x0000ff00) >> 8

    def get_min_size(self):
        return (self.mask & 0x00ff0000) >> 16

    def get_max_size(self):
        return (self.mask & 0xff000000) >> 24

    def dump(self, id, rules_map, types_map, devices_map):
        print "rule %s {" % rules_map[str(id)]
        print "\truleset %d" % self.get_ruleset()
        print "\ttype %s" % CEPF_PG_TYPE[self.get_type()]
        print "\tmin_size %d" % self.get_min_size()
        print "\tmax_size %d" % self.get_max_size()
        for s in self.steps:
            s.dump(types_map, devices_map)
        print "}"


class CrushDict(object):
    def __init__(self, d):
        self._d = d

    def dump(self, prefix):
        for key in sorted(dict.keys()):
            print "%s %s %s" % (prefix, key, self._d[key])


class CrushBucket(Struct):
    _fields_ = [('id', c_int32),  # /* this'll be negative */
                ('type', c_uint16),  # /* non-zero; type=0 is reserved for devices */
                ('alg', c_uint8),  # /* one of CRUSH_BUCKET_* */
                ('hash', c_uint8),  # /* which hash function to use, CRUSH_HASH_* */
                ('weight', c_uint32),  # /* 16-bit fixed point */
                ('size', c_uint32),  # /* num items */
                # ('item', c_void_p),

                # cached random permutation
                ('perm_x', c_uint32),
                ('perm_n', c_uint32)
                # ('perm', c_void_p)\
                ]

    def __init__(self):
        self.item = None
        self.perm = None

    def get_item_weight(self, id):
        raise NotImplementedError

    def dump(self, types, devices, rules):
        type_name = types[str(self.type)]
        device_name = devices[str(self.id)]
        print "%s %s {" % (type_name, device_name)
        print "\tid %d\t\t# do not change unnecessarily" % self.id
        print "\t# weight %.3f" % (self.weight >> 16)
        print "\talg %s" % CRUSH_ALG_NAME[self.alg]
        print "\thash %d\t# rjenkins1" % self.hash
        for index in xrange(0, self.size):
            item_id = self.item[index]
            item_name = get_item_name(self.item[index], devices)  # devices[str(item_id)]
            item_weight = self.get_item_weight(index)
            print "\titem %s weight %.3f" % (item_name, (item_weight >> 16))
        print "}"


class CrushBucketUniform(CrushBucket):
    def __init__(self):
        self.item_weight = 0

    def get_item_weight(self, id):
        return self.item_weight


class CrushBucketList(CrushBucket):
    def __init__(self):
        self.item_weight = []
        self.sum_weight = []

    def get_item_weight(self, id):
        return self.item_weight[id]


class CrushBucketTree(CrushBucket):
    def __init__(self):
        self.num_nodes = 0
        self.node_weights = []

    def get_item_weight(self, id):
        return self.item_weight[id]


class CrushBucketStraw(CrushBucket):
    def __init__(self):
        self.item_weight = []
        self.straws = []

    def get_item_weight(self, id):
        return self.item_weight[id]


class CrushMap(Struct):
    _fields_ = [  # ('buckets', c_voidp),
        # ('rules', c_voidp),
        ('max_buckets', c_int32),
        ('max_rules', c_uint32),
        ('max_devices', c_int32),
        ('choose_local_tries', c_uint32),
        ('choose_local_fallback_tries', c_uint32),
        ('choose_total_tries', c_uint32),
        ('chooseleaf_descend_once', c_uint32),
        ('choose_tries', c_voidp)]

    def __init__(self):
        self.buckets = []
        self.rules = []


class CrushDecode(object):
    def __init__(self, path):
        self._fd = None
        if not os.path.exists(path):
            print u"文件不存在"
        else:
            self._fd = open(path, "rb")
        self.crush_map = CrushMap()

    def _read_u8(self):
        return struct.unpack("B", self._fd.read(1))[0]

    def _read_i8(self):
        return struct.unpack("b", self._fd.read(1))[0]

    def _read_u16(self):
        return struct.unpack("H", self._fd.read(2))[0]

    def _read_i16(self):
        return struct.unpack("h", self._fd.read(2))[0]

    def _read_u32(self):
        return struct.unpack("I", self._fd.read(4))[0]

    def _read_i32(self):
        return struct.unpack("i", self._fd.read(4))[0]

    def _read_cn(self, n):
        l = []
        for i in xrange(0, n):
            v = struct.unpack("c", self._fd.read(1))[0]
            l.append(v)
        return "".join(l)

    def dump(self):
        print "# begin crush map"

        print "\n# devices"
        self.dump_devices()

        print "\n# types"
        self.dump_types()

        print "\n# buckets"
        self.dump_buckets()

        print "\n# rules"
        self.dump_rules()

        print "\n# end crush map"

    def dump_buckets(self):
        for bucket in self.crush_map.buckets:
            if bucket:
                bucket.dump(self.types, self.devices, self.rules)

    def dump_rules(self):
        size = len(self.crush_map.rules)
        for i in xrange(0, size):
            rule = self.crush_map.rules[i]
            rule.dump(i, self.rules, self.types, self.devices)

    def dump_types(self):
        if not self.types.has_key("0"):
            print "type 0 device"
        for key in sorted(self.types.keys()):
            print "type %s %s" % (key, self.types[key])

    def dump_devices(self):
        #         for key in sorted(self.devices.keys()):
        #             if int(key) >= 0:
        #                 print "device %s %s" % (key, self.devices[key])
        for i in xrange(0, self.crush_map.max_devices):
            print "device %d %s" % (i, get_item_name(i, self.devices))

    def decode(self):
        self.decode_header()
        self.decode_all_buckets()
        self.decode_all_rules()
        self.types = self.decode_map()
        self.devices = self.decode_map()
        self.rules = self.decode_map()
        self.decode_tail()
        # print self.crush_map
        self.dump()

    def decode_header(self):
        magic = self._read_u32()
        self.crush_map.max_buckets = self._read_u32()
        self.crush_map.max_rules = self._read_u32()
        self.crush_map.max_devices = self._read_u32()
        if magic != CRUSH_MAGIC:
            raise Error("magic error")

    def decode_tail(self):
        self.crush_map.choose_local_tries = self._read_u32()
        self.crush_map.choose_local_fallback_tries = self._read_u32()
        self.crush_map.choose_total_tries = self._read_u32()
        self.crush_map.chooseleaf_descend_once = self._read_u32()

    def decode_map(self):
        dct = {}
        len = self._read_u32()
        for i in xrange(0, len):
            key = self._read_i32()
            vlen = self._read_u32()
            value = self._read_cn(vlen)
            dct[str(key)] = value
        # print dct
        return dct

    def decode_all_rules(self):
        for i in xrange(0, self.crush_map.max_rules):
            self.crush_map.rules.append(self.decode_rule())

    def decode_rule(self):
        yes = self._read_u32()
        if int(yes) == 0:
            return None
        rule = CrushRule()
        rule.len = self._read_u32()
        rule.mask = self._read_u32()
        for i in xrange(0, rule.len):
            step = CrushRuleStep()
            step.op = self._read_u32()
            step.arg1 = self._read_i32()
            step.arg2 = self._read_i32()
            rule.steps.append(step)
        return rule

    def decode_all_buckets(self):
        for i in xrange(0, self.crush_map.max_buckets):
            self.crush_map.buckets.append(self.decode_bucket())

    def decode_bucket(self):
        (alg,) = struct.unpack("I", self._fd.read(4))
        if alg == 0:
            return None
        if alg > len(CRUSH_BUCKET) - 1:
            raise Error("alg unknow")

        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_UNIFORM":
            bucket = CrushBucketUniform()
        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_LIST":
            bucket = CrushBucketList()
        if CRUSH_BUCKET[alg] == "CRUSH_BUC KET_TREE":
            bucket = CrushBucketTree()
        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_STRAW":
            bucket = CrushBucketStraw()

        bucket.id = self._read_i32()
        bucket.type = self._read_u16()
        bucket.alg = self._read_u8()
        bucket.hash = self._read_u8()
        bucket.weight = self._read_i32()
        bucket.size = self._read_u32()

        l = []
        for i in xrange(0, bucket.size):
            l.append(self._read_i32())
        bucket.item = l

        # print bucket

        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_UNIFORM":
            bucket.item_weight = self._read_u32()
        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_LIST":
            wlst = []
            slst = []
            for i in xrange(0, bucket.size):
                wlst.append(self._read_u32())
                slst.append(self._read_u32())
            bucket.item_weight = wlst
            bucket.sum_weights = slst
        if CRUSH_BUCKET[alg] == "CRUSH_BUC KET_TREE":
            bucket.num_nodes = self._read_u8()
            wlst = []
            for i in xrange(0, bucket.num_nodes):
                wlst.append(self._read_u32())
            bucket.node_weights = wlst
        if CRUSH_BUCKET[alg] == "CRUSH_BUCKET_STRAW":
            wlst = []
            slst = []
            for i in xrange(0, bucket.size):
                wlst.append(self._read_u32())
                slst.append(self._read_u32())
            bucket.item_weight = wlst
            bucket.straws = slst
        return bucket


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='decompile')
    args = parser.parse_args()
    if args.d:
        file = args.d
        cd = CrushDecode(file)
        cd.decode()
