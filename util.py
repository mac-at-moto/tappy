import json
import sys
import numpy as np

from IPython.display import HTML

def codeblock_toggle():
  return HTML('''
    <script>
      code_show=true; 
      function code_toggle() {
       if (code_show){
       $('div.input').hide();
       } else {
       $('div.input').show();
       }
       code_show = !code_show
      } 
      $( document ).ready(code_toggle);
    </script>
    <form action="javascript:code_toggle()"><input type="submit" value="Toggle on/off the code blocks."></form>
    ''')

def createNumpyArray(data):
  output = []
  for dS in data:
    d = json.loads(dS)
    output.append(d)
  return np.array(output)

if __name__ == "__main__":
  import sys
  parseJSONArray(sys.argv[1])
