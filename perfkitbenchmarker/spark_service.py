# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing spark service classes.

A spark service provides create, delete, exists, and submit job.
"""

import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS

# Cloud to use for pkb-created spark service.
PKB_MANAGED = 'pkb_managed'
PROVIDER_MANAGED = 'managed'

_SPARK_SERVICE_REGISTRY = {}


def GetSparkService(cloud):
  """Get the Spark class corresponding to 'cloud'."""
  if cloud in _SPARK_SERVICE_REGISTRY:
    return _SPARK_SERVICE_REGISTRY.get(cloud)
  else:
    registered_clouds = ' '.join(_SPARK_SERVICE_REGISTRY.keys())
    raise Exception('No spark service found for {0} Options are {1}'.format(
        cloud, registered_clouds))


class AutoRegisterSparkServiceMeta(abc.ABCMeta):
  """Metaclass which allows SparkServices to register"""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('BaseSparkService subclasses must have a CLOUD'
                        'attribute.')
      else:
        _SPARK_SERVICE_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterSparkServiceMeta, cls).__init__(name, bases, dct)



class BaseSparkServiceSpec(spec.BaseSpec):
  """Stores the information needed to create a spark service.

  Attributes:
    machine_type: Type of the machine.
    num_workers: Number of workers.
  """

  CLOUD = None

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
          modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    raise Exception('woo hoo?')
    super(BaseSparkSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['machine_type'].present:
      config_values['machine_type'] = flag_values.machine_type
    if flag_values['num_workers'].present:
      config_values['num_workers'] = flag_values.num_workers

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    raise Exception('woo hoo')
    result = super(BaseSparkService, cls)._GetOptionDecoderConstructions()
    result.update({
        'machine_type': (option_decoders.StringDecoder, {'default': None,
                                                         'none_ok': True}),
        'num_workers': (option_decoders.IntDecoder, {'default': None,
                                                     'none_ok': True})})
    return result


class BaseSparkService(resource.BaseResource):
  """Object representing a Spark Service."""

  __metaclass__ = AutoRegisterSparkServiceMeta

  def __init__(self, name, spark_service_spec):
    super(BaseSparkService, self).__init__()
    self.name = name
    self.num_workers = spark_service_spec.num_workers
    self.machine_type = spark_service_spec.machine_type
    self.project = spark_service_spec.project
    if name.startswith('pkb-'):
      self.static_cluster = False
    else:
      self.static_cluster = True

  @abc.abstractmethod
  def SubmitJob(self, job_jar, class_name):
    pass

  @abc.abstractmethod
  def SetClusterProperty(name, value):
    pass


class PkbSparkService(BaseSparkService):
  """A spark service created from vms."""

  CLOUD = PKB_MANAGED
  vms = []

  def __init__(self, name, spark_service_spec):
    """Initializes a StripedDisk object.

    Args:
      spark_service_spec: specification of the spark service
    """
    super(PkbSparkService, self).__init__(name, spark_service_spec)


  def _Create(self):
    """Create a spark cluster."""
    raise NotImplementedError()

  def _Delete(self):
    """Delete the vms"""
    for vm in vms:
      vm.delete()

  # TODO(hildrum) actually implement this.
  def SubmitJob(self, jar_file, class_name):
    """Submit the jar file."""
    pass

  def SetClusterProperty(name, value):
    """Set a property to be used to create the cluster"""
    pass
