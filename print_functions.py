def big_statement(statement):

    star = 7
    space = 5
    dot = 2*(star + space) + statement.__len__()
    return  "\n" + dot*"-" +"\n" + star*"*" + space*" " + statement + space*" " + star*"*" + "\n" + dot*"-" + "\n"


def time_format(seconds):

    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    out = " "

    if hours > 0:
        out = out + str(hours) + " h "
        minutes = minutes % 60

    if minutes > 0:
        out = out + str(minutes) + " m "
        seconds = seconds % 60

    out = out + str(seconds) + " s"

    return out;