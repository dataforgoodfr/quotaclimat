{# Définitions communes aux tables du schéma curate : une colonne présente dans plusieurs tables y a la même
   description, par {{ doc('nom') }} dans _curate.yml. #}

{% docs curate_chaine_id %}
Identifiant technique de la chaîne (ex. `bfmtv`, `france-inter`), clé de jointure avec la table `curate.chaine`. Pour afficher le nom de la chaîne, utiliser `chaine_nom`.
{% enddocs %}

{% docs curate_chaine_nom %}
Nom de la chaîne tel qu'affiché dans les publications de l'Observatoire (ex. « BFM TV », « France Inter »). Une chaîne n'a qu'un nom : on peut regrouper par cette colonne.
{% enddocs %}

{% docs curate_est_service_public %}
Vrai pour une chaîne du service public (France Télévisions, Radio France, Arte, France Médias Monde en France).
{% enddocs %}

{% docs curate_est_info_continue %}
Vrai pour une chaîne d'information en continu (BFM TV, CNews, LCI, franceinfo...).
{% enddocs %}

{% docs curate_est_radio %}
Vrai pour une radio, faux pour une chaîne de télévision.
{% enddocs %}

{% docs curate_jour %}
Jour de diffusion, en heure de Paris. À utiliser pour regrouper par jour, semaine ou mois.
{% enddocs %}
