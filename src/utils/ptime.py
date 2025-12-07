def ptime(n):
    if n >= 60:
        return f"{round(n / 60 , 1)} mins"
    if n < 60:
        return f"{round(n , 1)} secs"
