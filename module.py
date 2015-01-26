import json
import requests
import sys
import time

tapServer = 'http://localhost:8090/'

def connect(server):
  global tapServer
  tapServer = server

def createContext(contextName):
  """
  Create a context object.
  The context name should be alphanumeric without any special characters.
  """
  global tapServer
  q = tapServer + 'contexts/' + contextName
  r = requests.post(q)
  return r

def deleteContext(contextName):
  """
  Delete the context object and free up all backend resources allocated to this context.
  """
  global tapServer
  q = tapServer + 'contexts/' + contextName
  r = requests.delete(q)
  return r

def run(contextName, classPath, conf, sync=True):
  """
  Execute the module.

  contextName -- the name of the context (environment)
  classPath -- the fully qualified class name of the target module.
  conf -- the module configuration string. It could be Java properties, JSON or HOCON.
  sync -- default to True. If set to False,
  this call will return a jobid instead of the actual output.
  """
  global tapServer
  q = tapServer + 'jobs?' + 'appName=tap&context=' + contextName + '&classPath=' + classPath
  if (sync):
    q += '&sync=true'
  else:
    q += '&sync=false'
  r = requests.post(q, data = conf)
  return json.loads(r.text)

def getJobOutput(jobId, timeout=300):
  """
  Use this method to wait and retrieve output of an async module execution.

  jobId -- obtained by invoking the "run" method in async mode.
  timeout -- this is how many seconds this method will wait before timing out. Default is 300.
  """
  global tapServer
  q = tapServer + 'jobs/' + jobId
  r = requests.get(q)
  resp = json.loads(r.text)
  status = resp['status']
  count = 0
  while (status != 'OK' and status != 'ERROR' and count < timeout):
    r = requests.get(tapServer + 'jobs/' + jobId)
    resp = json.loads(r.text)
    status = resp['status']
    time.sleep(1)
    count += 1
    sys.stdout.write('.')
    sys.stdout.flush()
  print
  return json.loads(r.text)


if __name__ == "__main__":
  import sys
  run(sys.argv[1], sys.argv[2], True)
