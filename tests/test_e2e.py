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

def submit(func: Callable[..., Any]) -> int:
    task = Task

if __name__ == "__main__":
    start_workers(2)
    func = make_func()
    submit(func, {"param": "abc"})
    sleep(3)
