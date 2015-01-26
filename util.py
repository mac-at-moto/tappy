import json
import sys
import numpy as np

def createNumpyArray(data):
  output = []
  for dS in data:
    d = json.loads(dS)
    output.append(d)
  return np.array(output)

if __name__ == "__main__":
  import sys
  parseJSONArray(sys.argv[1])
