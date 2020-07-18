from time import sleep
from typing import Any, Callable


def make_func() -> Callable[[str], int]:
    class ClosuredClass:
        def __init__(self, x: int) -> None:
            self.x = x
    closured_obj = ClosuredClass(x=42)
    
    def closured(param: str) -> int:
        return len(param) + closured_obj.x 

    return closured   



if __name__ == "__main__":
    print(2)
    # start_workers(2)
    # func = make_func()
    # task_id = submit(func, {"param": "abc"})
    # sleep(3)
    # task = collect(task_id)
