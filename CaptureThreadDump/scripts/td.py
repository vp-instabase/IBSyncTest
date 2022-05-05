import sys
import traceback
import threading
import time


def stacktraces(**kwargs):


  id2name = {}
  for th in threading.enumerate():
    id2name[th.ident] = th.name

  code = []
  
  for _ in range(5):
    for threadId, stack in sys._current_frames().items():
      code.append("\n# Thread: %s(%d)" % (id2name[threadId], threadId))
      for filename, lineno, name, line in traceback.extract_stack(stack):
        code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
        if line:
          code.append("  %s" % (line.strip()))
    time.sleep(3)
  return "\n".join(code)


def register(name_to_fn):
  more_fns = {
      'stacktraces': {
          'fn': stacktraces,
          'ex': '',
          'desc': ''
      }
  }
  name_to_fn.update(more_fns)