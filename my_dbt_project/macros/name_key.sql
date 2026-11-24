{#- Join key of a brand or group name, the SQL version of nospace(normalize(name)) in
    quotaclimat/data_ingestion/advertising/tools/normalize.py:
    accents stripped, œ/æ spelled out, subscripts as digits, lower case, punctuation and spaces
    removed ("L’Oréal Paris" -> "lorealparis", "Cœur de Lion" -> "coeurdelion").
    Does not depend on the database collation: the accented letters of the Latin-1 and Latin Extended-A
    blocks are mapped explicitly (table generated from the Python function), ASCII letters are lowered,
    then everything but a-z, 0-9, _ and the remaining Latin letters (ø, ß...) is removed.
    Differs from Python for other scripts (Greek, Cyrillic...), removed here but kept by Python.
    Checked against the Python function by test_name_key_matches_python (pytest_tests). -#}
{% macro name_key(column) -%}
  regexp_replace(
    replace(replace(lower(translate(
      {{ column }},
      'ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞàáâãäåçèéêëìíîïñòóôõöùúûüýÿĀāĂăĄąĆćĈĉĊċČčĎďĐĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦĨĩĪīĬĭĮįİĲĴĵĶķĹĺĻļĽľĿŁŃńŅņŇňŊŌōŎŏŐőŒŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŦŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽž₀₁₂₃₄₅₆₇₈₉²³',
      'aaaaaaæceeeeiiiiðnoooooøuuuuyþaaaaaaceeeeiiiinooooouuuuyyaaaaaaccccccccddđeeeeeeeeeegggggggghhħiiiiiiiiiĳjjkkllllllŀłnnnnnnŋooooooœrrrrrrssssssssttttŧuuuuuuuuuuuuwwyyyzzzzzz012345678923'
    )), 'œ', 'oe'), 'æ', 'ae'),
    '[^a-z0-9_À-ÖØ-öø-ɏ]',
    '',
    'g'
  )
{%- endmacro %}
