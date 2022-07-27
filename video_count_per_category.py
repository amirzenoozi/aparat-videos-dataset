from bidi.algorithm import get_display

import matplotlib.pyplot as plt

import arabic_reshaper
import sqlite3

def reshape_arabic_text(text):
    return get_display(arabic_reshaper.reshape(text))

def main():
    labels = []
    values = []
    con = sqlite3.connect('aparat.db')
    records = con.execute("""SELECT name, video_count FROM categories""").fetchall()
    for record in records:
        labels.append(reshape_arabic_text(record[0]))
        values.append(record[1])

    patches = plt.pie(values, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    plt.legend(patches, labels, loc="best")
    # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.axis('equal')
    plt.show()
    con.close()

if __name__ == '__main__':
    main()