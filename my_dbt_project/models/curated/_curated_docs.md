{# Définitions communes aux tables du schéma curated : une colonne présente dans plusieurs tables y a la même
   description, par {{ doc('nom') }} dans _curated.yml. #}

{% docs curated_chaine_id %}
Identifiant technique de la chaîne (ex. `bfmtv`, `france-inter`), clé de jointure avec la table `curated.chaine`. Pour afficher le nom de la chaîne, utiliser `chaine_nom`.
{% enddocs %}

{% docs curated_chaine_nom %}
Nom de la chaîne tel qu'affiché dans les publications de l'Observatoire (ex. « BFM TV », « France Inter »). Une chaîne n'a qu'un nom : on peut regrouper par cette colonne.
{% enddocs %}

{% docs curated_est_service_public %}
Vrai pour une chaîne du service public (France Télévisions, Radio France, Arte, France Médias Monde en France).
{% enddocs %}

{% docs curated_est_info_continue %}
Vrai pour une chaîne d'information en continu (BFM TV, CNews, LCI, franceinfo...).
{% enddocs %}

{% docs curated_est_radio %}
Vrai pour une radio, faux pour une chaîne de télévision.
{% enddocs %}

{% docs curated_jour %}
Jour de diffusion, en heure de Paris. À utiliser pour regrouper par jour, semaine ou mois.
{% enddocs %}

{% docs curated_emission_jour_id %}
Identifiant d'une émission un jour donné, clé de jointure avec la table `curated.emission_jour` (forme : chaîne|type|nom|rediffusion|jour).
{% enddocs %}

{% docs curated_duree_theme %}
Durée en minutes où ce sujet est évoqué dans l'émission ce jour-là : nombre de fenêtres de 20 secondes contenant au moins un mot-clé du sujet, multiplié par 20 secondes. Se somme sur les jours et les émissions. Une même fenêtre peut contenir plusieurs sujets : les sujets ne s'additionnent pas pour donner `duree_environnement_min`.
{% enddocs %}

{% docs curated_part %}
Part du temps suivi consacrée à ce sujet, de 0 à 100, pour cette émission ce jour-là. Ne pas en faire la moyenne sur plusieurs lignes : pour une période, une chaîne ou un groupe d'émissions, sommer la durée du sujet et `duree_suivie_min`, puis diviser (x 100).
{% enddocs %}
