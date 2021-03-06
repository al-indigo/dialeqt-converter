from flask import Flask
import sqlite3
import psycopg2
import re
import sys
import os
import subprocess
import glob

app = Flask(__name__)


def convert_db(sqconn, pgconn, blobs_path):
    dict_author = unicode('')
    dict_coauthors = unicode('')
    dict_name = unicode('')
    dict_tags = unicode('')
    dict_description = unicode('')
    dict_identifier = unicode('')

    author_id = 0
    dict_id = 0

    dict_trav = sqconn.cursor()
    dict_trav.execute("""SELECT
                        dict_author,
                        dict_coauthors,
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
        dict_coauthors = dictionary[1]
        dict_name = dictionary[2]
        dict_tags = dictionary[3]
        dict_description = dictionary[4]
        dict_identifier = dictionary[5]

    print "Got info about dictionary:"
    print dict_author
    print dict_name
    print dict_tags
    print dict_description
    print dict_identifier

    if not any([dict_author, dict_coauthors, dict_name, dict_tags, dict_description, dict_id]):
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
        print "Inserted, id = ", author_id
    else:
        print "Author found, continue", author_id

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
        print "Dict found, continue", dict_id

    coauthor_list = dict_coauthors.split(",")
    coauthor_list.insert(0, dict_author)

    for coauthor in coauthor_list:
        print coauthor
        if coauthor == "" or coauthor == "''":
            continue
        coauthor_id = 0
        coauthor_search = pgconn.cursor()
        coauthor_search.execute("""SELECT
                                id
                                FROM
                                authors
                                WHERE
                                name = (%s);
                                """, [coauthor])
        for authors in coauthor_search:
            coauthor_id = authors[0]
        if coauthor_id == 0:
            print "No authors matched, preparing to insert"
            coauthor_insert = pgconn.cursor()
            coauthor_insert.execute("""INSERT INTO
                                    authors
                                    (name)
                                    VALUES
                                    (%s)
                                    RETURNING id
                                    ;""", [coauthor])
            pgconn.commit()
            for author in coauthor_insert:
                coauthor_id = author[0]
            if coauthor_id == 0:
                return 3, "Can't find and even insert author, it's bad"
            print "Inserted, id = ", author_id
        else:
            print "Author found, continue"
            print coauthor_id
        coauthor_link_search = pgconn.cursor()
        coauthor_link_search.execute("""SELECT
                                        dictionary_id,
                                        author_id
                                        FROM
                                        authors_dictionaries
                                        WHERE
                                         dictionary_id = (%s) AND author_id = (%s);
                                        """, [dict_id, coauthor_id])
        found_coauthor = False
        for item in coauthor_link_search:
            found_coauthor = True
        if not found_coauthor:
            coauthor_link_insert = pgconn.cursor()
            coauthor_link_insert.execute("""INSERT INTO
                                            authors_dictionaries (dictionary_id, author_id)
                                            VALUES
                                            (%s, %s)
                                            ;""", [dict_id, coauthor_id])
            pgconn.commit()

    word_traversal = sqconn.cursor()
    word_traversal.execute("""SELECT
                            id,
                            word,
                            regular_form,
                            transcription,
                            translation,
                            etimology_tag,
                            is_a_regular_form
                            FROM
                            dictionary
                            ;""")
    words_total = 0
    words_inserted = 0
    words_failed = 0
    for sqword in word_traversal:
        words_total += 1
        word_id = sqword[0]
        word = unicode(sqword[1])
        regform = unicode(sqword[2])
        transcription = unicode(sqword[3])
        translation = unicode(sqword[4])
        etimtag = unicode(sqword[5])
        is_a_regular_form = sqword[6]
        if etimtag == "None":
            etimtag = ""

        pgword_id = 0
        pgparadigm_id = 0

        is_word = True
        print "id:", word_id, " word: ", word, " regform: ", regform, " transcription: ", transcription, " translation: ",  translation, " tag: ", etimtag
        if regform != word_id and regform != "":
            print "It's not a word"
            is_word = False
            if is_a_regular_form == 1:
                is_word = True
        if is_word:
            print "Word!"

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
                                    paradigms
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
                if pgword_id == 0:
                    words_failed += 1
                else:
                    words_inserted += 1

        if not is_word:
            if pgparadigm_id == 0:
                original_find = sqconn.cursor()
                original_find.execute("""SELECT
                                        word,
                                        transcription,
                                        translation,
                                        etimology_tag
                                        FROM
                                        dictionary
                                        WHERE
                                        id = (?);
                                        """, [regform])
                original_word_tuple = None
                for original_word in original_find:
                    original_word_tuple = [original_word[0],
                                           original_word[1],
                                           original_word[2],
                                           original_word[3]]

                if not original_word_tuple:
                    print("Minor failure: orphaned paradigm.")
                    words_failed += 1
                    continue

                find_corresponding_word = pgconn.cursor()
                find_corresponding_word.execute("""SELECT
                                                id
                                                FROM
                                                words
                                                WHERE
                                                transcription = (%s)
                                                """, [original_word_tuple[1]])
                originated = 0
                for orig in find_corresponding_word:
                    originated = orig[0]
                if originated == 0:
                    print ("Minor inconsistency; skip", transcription, regform)
                    words_failed += 1
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
                if pgparadigm_id == 0:
                    words_failed += 1
                else:
                    words_inserted += 1

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
                #print "Inconsistent blob description, skipping"
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

            #if is_word:
            if blobtype == 1:
                have_found = False
                check_blob = pgconn.cursor()
                if is_word:
                    check_blob.execute("""SELECT
                                        id
                                        FROM
                                        word_sounds
                                        WHERE
                                        sound_file_name = (%s) AND word_id = (%s)
                                        """, [blobname, pgword_id])
                else:
                    check_blob.execute("""SELECT
                                        id
                                        FROM
                                        paradigm_sounds
                                        WHERE
                                        sound_file_name = (%s) AND paradigm_id = (%s)
                                        """, [blobname, pgparadigm_id])
                
                for found in check_blob:
                    if found[0]:
                        have_found = True
                if have_found:
                    #print "We have this blob already"
                    continue
                if blobname == "":
                    continue

                insert_blob = pgconn.cursor()
                if is_word:
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
                else:
                    insert_blob.execute("""INSERT INTO
                                        paradigm_sounds
                                        (paradigm_id,
                                        description,
                                        sound_file_name,
                                        sound_content_type,
                                        sound_file_size)
                                        VALUES
                                        (%s, %s, %s, %s, %s)
                                        RETURNING id
                                        """, [pgparadigm_id, blobdescription, blobname, "sound/wav", len(mainblob)])
                pgconn.commit()

                pgblobid = 0
                for inserted in insert_blob:
                    pgblobid = inserted[0]

                re.findall('...', str(pgblobid).zfill(9))
                if is_word:
                    base_path = blobs_path + "word_sounds/sounds/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                else:
                    base_path = blobs_path + "paradigm_sounds/sounds/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                write_wav_blob_path = base_path + '/original/'
                write_mp3_blob_path = base_path + '/mp3/'
                if not os.path.exists(write_wav_blob_path):
                    os.makedirs(write_wav_blob_path)
                if not os.path.exists(write_mp3_blob_path):
                    os.makedirs(write_mp3_blob_path)

                write_wav_blob_path += "/" + blobname
                #print write_wav_blob_path

                with open(write_wav_blob_path, "w") as f:
                    f.write(mainblob)
                f.close()

                mp3blobname = None
#                    print blobname
                if os.path.splitext(os.path.basename(blobname))[1] == ".wav":
                    mp3blobname = os.path.splitext(os.path.basename(blobname))[0] + ".mp3"

                if mp3blobname is not None:

                    wav = write_wav_blob_path
                    mp3 = write_mp3_blob_path + mp3blobname
                    cmd = 'lame --preset insane "%s" "%s"' % (wav, mp3)
                    print cmd
                    DEVNULL = open(os.devnull, 'wb')
                    subprocess.call(cmd, shell=True, stdout=DEVNULL)
            if blobtype == 2:
                have_found = False
                check_blob = pgconn.cursor()
                if is_word:
                    check_blob.execute("""SELECT
                                        id
                                        FROM
                                        word_praats
                                        WHERE
                                        sound_file_name = (%s) AND word_id = (%s)
                                        """, [blobname + ".wav", pgword_id])
                else:
                    check_blob.execute("""SELECT
                                        id
                                        FROM
                                        paradigm_praats
                                        WHERE
                                        sound_file_name = (%s) AND paradigm_id = (%s)
                                        """, [blobname + ".wav", pgparadigm_id])
                
                for found in check_blob:
                    if found[0]:
                        have_found = True
                if have_found:
                    #print "We have this blob already"
                    continue
                if blobname == "":
                    continue
                if mainblob is None or secblob is None:
                    print "Broken markup! ", blobname
                    continue

                insert_blob = pgconn.cursor()
                if is_word:
                    insert_blob.execute("""INSERT INTO
                                        word_praats
                                        (word_id,
                                        description,
                                        sound_file_name,
                                        sound_content_type,
                                        markup_file_name,
                                        markup_content_type,
                                        sound_file_size,
                                        markup_file_size)
                                        VALUES
                                        (%s, %s, %s, %s, %s, %s, %s, %s)
                                        RETURNING id
                                        """, [pgword_id,
                                              blobdescription,
                                              blobname + ".wav",
                                              "sound/wav",
                                              blobname + ".TextGrid",
                                              "markup/praat",
                                              len(mainblob),
                                              len(secblob)])
                else:
                    insert_blob.execute("""INSERT INTO
                                        paradigm_praats
                                        (paradigm_id,
                                        description,
                                        sound_file_name,
                                        sound_content_type,
                                        markup_file_name,
                                        markup_content_type,
                                        sound_file_size,
                                        markup_file_size)
                                        VALUES
                                        (%s, %s, %s, %s, %s, %s, %s, %s)
                                        RETURNING id
                                        """, [pgparadigm_id,
                                              blobdescription,
                                              blobname + ".wav",
                                              "sound/wav",
                                              blobname + ".TextGrid",
                                              "markup/praat",
                                              len(mainblob),
                                              len(secblob)])
                pgconn.commit()

                pgblobid = 0
                for inserted in insert_blob:
                    pgblobid = inserted[0]

                re.findall('...', str(pgblobid).zfill(9))
                if is_word:
                    base_sound_path = blobs_path + "word_praats/sounds/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                    base_markup_path = blobs_path + "word_praats/markups/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                else:
                    base_sound_path = blobs_path + "paradigm_praats/sounds/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                    base_markup_path = blobs_path + "paradigm_praats/markups/" + '/'.join(re.findall('...', str(pgblobid).zfill(9)))
                write_wav_blob_path = base_sound_path + '/original/'
                write_textgrid_blob_path = base_markup_path + '/original/'
                if not os.path.exists(write_wav_blob_path):
                    os.makedirs(write_wav_blob_path)
                if not os.path.exists(write_textgrid_blob_path):
                    os.makedirs(write_textgrid_blob_path)

                write_wav_blob_path += "/" + blobname + ".wav"
                #print write_wav_blob_path
                with open(write_wav_blob_path, "w") as f:
                    f.write(mainblob)
                f.close()

                write_textgrid_blob_path += "/" + blobname + ".TextGrid"
                print "Have markup: ", write_textgrid_blob_path
                with open(write_textgrid_blob_path, "w") as f:
                    f.write(secblob)
                f.close()


    print "SUMMARY:"
    print "words total: ", words_total
    print "words inserted: ", words_inserted
    print "words failed: ", words_failed
    return 0, "convert successful!"


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    blobs_path = "/home/al/dialeqt-on-rails/public/system/"
    pgdbname = "dialeqt-on-rails_development"
    pgpassword = "d"
    sqlitefilelist = glob.glob(sys.argv[1] + "/*.sqlite")
    for sqlitefile in sqlitefilelist:
        sqconn = sqlite3.connect(sqlitefile)
        pgconn = psycopg2.connect(database=pgdbname, user="dialeqt-on-rails", password=pgpassword, host="localhost", port="5432")

    ##print
        (status, explain) = convert_db(sqconn, pgconn, blobs_path)
        print status, explain



#    app.run()
