def big_statement(statement):

    statements = statement.split("\n")
    maxLen = 0
    for s in statements:
        if s.__len__() > maxLen:
            maxLen = s.__len__()

    star = 7
    space = 5
    dot = 2*(star + space) + maxLen

    output = "\n" + dot*"-" +"\n"
    for s in statements:
        n = (maxLen - s.__len__())/2
        output += star*"*" + (n + space)*" " + s + (maxLen - s.__len__() - n + space)*" " + star*"*" + "\n"

    return (output + dot*"-" + "\n")


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