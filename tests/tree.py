import random
import time
decs = ["*", "@", "~"]
while True:
    for i in range(11):
        dec_left = random.choice(decs)
        dec_right = random.choice(decs)
        spaces_left = " " * (11 - (i - 1))
        inside_tree = "~" * (i * 2)
        print(f"{spaces_left}{dec_left}</{inside_tree}/>{dec_right}")
    for i in range(3):
        print(f"{' ' * 11}{'#' * 6}")
    print("\n\n\n\n\n\n\n\n\n\n")
    time.sleep(0.5)
