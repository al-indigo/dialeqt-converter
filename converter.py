from flask import Flask
import sqlite3
import psycopg2
import re
import sys
import os

app = Flask(__name__)


def convert_db(sqconn, pgconn, blobs_path):
    dict_author = unicode('')
    dict_name = unicode('')
    dict_tags = unicode('')
    dict_description = unicode('')
    dict_identifier = unicode('')

    author_id = 0
    dict_id = 0

    dict_trav = sqconn.cursor()
    dict_trav.execute("""SELECT
                        dict_author,
                        dict_name,
                        dict_classification_tags,
                        dict_description,
                        dict_identificator
                        FROM
                        dict_attributes
                        WHERE
                        id = 1;""")
    for dictionary in dict_trav:
        dict_author = dictionary[0]
        dict_name = dictionary[1]
        dict_tags = dictionary[2]
        dict_description = dictionary[3]
        dict_identifier = dictionary[4]

    print "Got info about dictionary:"
    print dict_author
    print dict_name
    print dict_tags
    print dict_description
    print dict_identifier

    if not any([dict_author, dict_name, dict_tags, dict_description, dict_id]):
        return 2, "It's not a Dialeqt dictionary"

    author_search = pgconn.cursor()
    author_search.execute("""SELECT
                            id
                            FROM
                            authors
                            WHERE
                            name = (%s);
                            """, [dict_author])
    for authors in author_search:
        author_id = authors[0]
    if author_id == 0:
        print "No authors matched, preparing to insert"
        author_insert = pgconn.cursor()
        author_insert.execute("""INSERT INTO
                                authors
                                (name)
                                VALUES
                                (%s)
                                RETURNING id
                                ;""", [dict_author])
        pgconn.commit()
        for author in author_insert:
            author_id = author[0]
        if author_id == 0:
            return 3, "Can't find and even insert author, it's bad"
        print "Inserted, id = "
        print author_id
    else:
        print "Author found, continue"
        print author_id

    dict_search = pgconn.cursor()
    dict_search.execute("""SELECT
                        id
                        FROM
                        dictionaries
                        WHERE
                        identifier = (%s);
                        """, [dict_identifier])
    for dicts in dict_search:
        dict_id = dicts[0]
    if dict_id == 0:
        print "Dictionary doesn't exist, creating"
        dict_insert = pgconn.cursor()
        dict_insert.execute("""INSERT INTO
                            dictionaries
                            (name, description, identifier, author_id)
                            VALUES
                            (%s, %s, %s, %s)
                            RETURNING id
                            ;""", [dict_name, dict_description, dict_identifier, author_id])
        pgconn.commit()
        for dict in dict_insert:
            dict_id = dict[0]
        if dict_id == 0:
            return 4, "Can't find dict and even insert"
        print "Inserted dict successfully"
    else:
        print "Dict found, continue"
        print dict_id

    word_traversal = sqconn.cursor()
    word_traversal.execute("""SELECT
                            id,
                            word,
                            regular_form,
                            transcription,
                            translation,
                            etimology_tag
                            FROM
                            dictionary
                            ;""")

    for sqword in word_traversal:
        word_id = sqword[0]
        word = unicode(sqword[1])
        regform = unicode(sqword[2])
        transcription = unicode(sqword[3])
        translation = unicode(sqword[4])
        etimtag = unicode(sqword[5])
        if etimtag == "None":
            etimtag = ""

        pgword_id = 0
        pgparadigm_id = 0

        is_word = True
        if regform != transcription and regform != "":
            is_word = False

        if is_word:
            find_word_in_pg = pgconn.cursor()
            find_word_in_pg.execute("""SELECT
                                    id
                                    FROM
                                    words
                                    WHERE
                                    tag = (%s) AND transcription = (%s)
                                    AND translation = (%s)
                                    AND word = (%s)
                                    AND dictionary_id = (%s)
                                    """, [etimtag, transcription, translation, word, dict_id])
            for item in find_word_in_pg:
                pgword_id = item[0]
        else:
            find_word_in_pg = pgconn.cursor()
            find_word_in_pg.execute("""SELECT
                                    id
                                    FROM
                                    words
                                    WHERE
                                    transcription = (%s)
                                    AND translation = (%s)
                                    AND word = (%s)
                                    """, [transcription, translation, word])
            for item in find_word_in_pg:
                pgparadigm_id = item[0]

        if is_word:
            if pgword_id == 0:
                insert_word_in_pg = pgconn.cursor()
                insert_word_in_pg.execute("""INSERT INTO
                                            words
                                            (word, transcription, translation, tag, dictionary_id)
                                            VALUES
                                            (%s, %s, %s, %s, %s)
                                            returning id
                                            """, [word, transcription, translation, etimtag, dict_id])
                pgconn.commit()
                for res in insert_word_in_pg:
                    pgword_id = res[0]

        if not is_word:
            if pgparadigm_id == 0:
                find_corresponding_word = pgconn.cursor
                find_corresponding_word.execute("""SELECT
                                                id
                                                FROM
                                                words
                                                WHERE
                                                transcription = (%s)
                                                """, [regform])
                originated = 0
                for orig in find_corresponding_word:
                    originated = orig[0]
                if originated == 0:
                    print ("Minor inconsistency; skip")
                    continue

                insert_word_in_pg = pgconn.cursor()
                insert_word_in_pg.execute("""INSERT INTO
                                            paradigms
                                            (word, transcription, translation, word_id)
                                            VALUES
                                            (%s, %s, %s, %s)
                                            returning id
                                            """, [word, transcription, translation, orig])
                pgconn.commit()
                for res in insert_word_in_pg:
                    pgparadigm_id = res[0]

        attachments_find = sqconn.cursor()
        attachments_find.execute("""SELECT
                                blobid,
                                type,
                                name,
                                description
                                FROM dict_blobs_description
                                WHERE
                                wordid = (?);
                                """, [word_id])

        for attach_description in attachments_find:
            blobid = attach_description[0]
            blobtype = attach_description[1]
            blobname = unicode(attach_description[2])
            blobdescription = unicode(attach_description[3])
            mainblob = ""
            secblob = ""

            if not any([blobid, blobtype, blobname, blobdescription]):
                print "Inconsistent blob description, skipping"
                continue

            # blobtype sound - 1 ; praat - 2
            get_blobs = sqconn.cursor()
            get_blobs.execute("""SELECT
                                mainblob,
                                secblob
                                FROM blobs
                                WHERE id = (?);
                                """, [blobid])

            for blobs in get_blobs:
                mainblob = blobs[0]
                secblob = blobs[1]

            if is_word:
                if blobtype == 1:
                    have_found = False
                    check_blob = pgconn.cursor()
                    check_blob.execute("""SELECT
                                        id
                                        FROM
                                        word_sounds
                                        WHERE
                                        sound_file_name = (%s) AND word_id = (%s)
                                        """, [blobname, pgword_id])
                    for found in check_blob:
                        if found[0]:
                            have_found = True
                    if have_found:
                        print "We have this blob already"
                        continue
                    if blobname == "":
                        continue

                    insert_blob = pgconn.cursor()
                    insert_blob.execute("""INSERT INTO
                                        word_sounds
                                        (word_id,
                                        description,
                                        sound_file_name,
                                        sound_content_type,
                                        sound_file_size)
                                        VALUES
                                        (%s, %s, %s, %s, %s)
                                        RETURNING id
                                        """, [pgword_id, blobdescription, blobname, "sound/wav", len(mainblob)])
                    pgconn.commit()

                    pgblobid = 0
                    for inserted in insert_blob:
                        pgblobid = inserted[0]

                    re.findall('...', str(pgblobid).zfill(9))
                    write_blob_path = blobs_path + "word_sounds/sounds/" + '/'.join(re.findall('...', str(pgblobid).zfill(9))) + '/original/'
                    if not os.path.exists(write_blob_path):
                        os.makedirs(write_blob_path)

                    write_blob_path += "/" + blobname
                    print write_blob_path

                    with open(write_blob_path, "w") as f:
                        f.write(mainblob)
                    f.close()

    return 0, "convert successful!"


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    blobs_path = "/home/al/RubymineProjects/dialeqt-on-rails/public/system/"
    pgdbname = "dialeqt-on-rails_development"
    pgpassword = "d"
    sqlitefile = "/home/al/nizjam-v0.2.sqlite"
    sqconn = sqlite3.connect(sqlitefile)
    pgconn = psycopg2.connect(database=pgdbname, user="dialeqt-on-rails", password=pgpassword, host="localhost", port="5432")

    print convert_db(sqconn, pgconn, blobs_path)

#    app.run()
