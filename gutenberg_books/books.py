#!/usr/bin/env python
import glob
from hashlib import md5
import avro.schema
from avro.datafile import DataFileWriter
from avro.datafile import DataFileReader

from avro.io import DatumWriter
from avro.io import DatumReader

def is_not_digit(path):
    just_name = path.split("/")[-1].split(".")[0]
    return not just_name.isdigit()

def not_good_file(path):
    return "-" in path or is_not_digit(path)

def md5_hash(k):
    return md5(k.encode()).hexdigest()

def get_text(path):
    with open(path) as book:
        return book.read()

def extract_term(term_indicator, text, default=None, max_term_size=75):
    term_start = text.find(term_indicator)
    # If not found, 
    if term_start == -1:
        term = default
    else:
        term_end = text.find("\n", term_start)
        term = text[term_start+len(term_indicator):term_end].strip()
    if term and (len(term) > max_term_size):
        term = default
    return term

def get_author_and_title(book_text):
    title = extract_term("Title:", book_text, default=None)
    author = extract_term("Author:", book_text, default=None)
    # Solve  for other strange author name formatting
    for term_indicator in ["\n\nby ", "\n\nOF ", "\nOF\n"]:
        if author is None:
            author = extract_term(term_indicator, book_text[:15000], max_term_size=25)
    return title, author

def locate_beginning_of_text(title, author, text):
    if title:
        location = text.find(title)
    if author:
        location = text.find(author)
    return location

def parse_book(book_text):
    """
    Given the text of a book, returns a list of dictionaries with the keys:
    {title, author, contents, part, hash}
    """
    parsed_book_paragraphs = []
    title, author = get_author_and_title(book_text)
    if title or author:
        # Get start of text position
        text_starts = locate_beginning_of_text(title, author, book_text)
        book_paragraphs = book_text[text_starts:].split("\n\n")
        for paragraph_number, raw_paragraph in enumerate(book_paragraphs):
            paragraph = raw_paragraph.replace("\n", " ").strip()
            book_data = {"title": title,
                         "author": author,
                         "contents": paragraph,
                         "part": paragraph_number,
                         "hash": md5_hash(paragraph)}
            parsed_book_paragraphs.append(book_data)
    return parsed_book_paragraphs            

def write_to_avro(book_paragraph_data, schema_location="books.avsc", output_location="book_data.avro"):
    """
    Write book paragraphs to some output location based on a defined schema.
    """
    schema = avro.schema.parse(open(schema_location).read())
    with DataFileWriter(open(output_location, "w"), DatumWriter(), schema) as writer:
        for paragraph in book_paragraph_data:
            writer.append(paragraph)


BOOK_DIRECTORY = "books"
# TODO: add this schema


if __name__ == "__main__":

    paragraphs = []

    for filename in list(glob.iglob(BOOK_DIRECTORY + '/*.txt')):
        path = filename.replace("\\", "/")

        # Skip files that are not books
        if not_good_file(path): continue

        # Parse book.
        book_text = get_text(path)
        parsed_book = parse_book(book_text)
        paragraphs.extend(parsed_book)
        
    # Write to avro
    write_to_avro(paragraphs)
    
    # Code to read from avro
    READ_BACK = False
    if READ_BACK:
        output_location = "book_data.avro"
        with DataFileReader(open(output_location, "r"), DatumReader()) as reader:
            for paragraph in reader:
                print paragraph