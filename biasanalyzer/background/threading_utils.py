import threading
import traceback

class BackgroundResult:
    def __init__(self):
        self.value = None
        self.error = None
        self.ready = False

    def set(self, result, error=None):
        self.value = result
        self.error = error
        self.ready = True

def run_in_background(func, *args, result_holder=None, on_complete=None, **kwargs):
    """
    Run a time-consuming function in background
    :param func: function to run in background
    :param args: function positional arguments to be passed in
    :param result_holder: BackgroundResult object to hold function returned result
    :param on_complete: a function to trigger to run automatically when this background process completes
    :param kwargs: any keyword arguments of the function to be passed in as a dict
    :return: a background thread
    """
    def wrapper():
        try:
            print("[*] Background task started...", flush=True)
            result = func(*args, **kwargs)
            print("[✓] Background task completed.", flush=True)
            if result_holder:
                result_holder.set(result)
            if on_complete:
                on_complete(result=result, error=None)
        except Exception as e:
            print("[!] Background task failed:", flush=True)
            traceback.print_exc()
            if result_holder:
                result_holder.set(None, error=e)
            if on_complete:
                on_complete(result=None, error=e)

    thread = threading.Thread(target=wrapper)
    thread.start()
    return thread
