# Incendies, été 2026

Analyse menée avant la révision de la méthode : le corpus noyau a été construit directement sur `public.keywords` (audiovisuel) et `public.factiva_articles` (presse), par regex dans les questions Metabase. Elle sert de modèle pour la démarche, les axes et la restitution.

Fichier reconstitué à partir des questions Metabase et de l'article publié. 🔴 [à relire par l'équipe qui a mené l'analyse]

- Statut : publié, [article du 22/09/2026](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/)
- Tables : `public.keywords`, `public.factiva_articles`. Pour les prochaines analyses incendies : entrée `incendies` de `thematic_dict.yml`.
- Collection Metabase : `03. Analyses thématiques/19. Incendies 2026` (id 362)

## 1. Extraction

### Termes du corpus noyau (regex des questions AV-Q2 et PE-Q2)

incendie(s), mégafeu(x), brasier, feu(x) de forêt, feu(x) de végétation, feu de tourbe, feu de lande, feu de broussailles, départ(s) de feu, reprise(s) de feu, front de flammes, front du feu, hectares brûlés, parti(s) en fumée, ravagé(e) par le feu, ravagée par les flammes, soldats du feu, bombardier(s) d'eau, Canadair.

Exclusion : incendie, feu ou brand(...) suivi à moins de 25 caractères de immeuble, maison, habitation, appartement, entrepôt, usine, hangar, garage, voiture, bus, camion, hôpital, école.

### Périodes

- Analyse : 1er juillet au 31 août 2026.
- Comparaison : janvier-avril 2026 (AV-Q6). 🔴 [vérifier la période de comparaison effectivement utilisée pour les chiffres publiés]

## 2. Axes

Voir [axes.md](axes.md) de ce dossier.

Ventilations publiées : public / privé, presse nationale / régionale.

## 3. Requêtes

| Question Metabase (id) | Ce qu'elle mesure | Dénominateur |
|---|---|---|
| AV - Q1 - Volumétrie (2204) / PE - Q1 (2218) | Volume du corpus | |
| AV - Q2 - Mentions du climat et aléas (2209) / PE - Q2 (2220) | Lien CC et co-aléas, par chaîne ou par titre | Segments ou articles du corpus |
| AV - Q3 - Causes (2214, 2266) / PE - Q3 (2221) | Causes | Segments ou articles du corpus |
| AV - Q5 - Solutions (2215, 2254) / PE - Q5 (2222) | Solutions | Segments ou articles du corpus |
| AV - Q6 - Baseline (2259, 2237, 2216, 2225) / PE - Q6 (2230, 2232) | Comparaison avec la période de référence | |
| AV - Q7 (2217, 2226) / PE - Q7 (2261) | Courbe journalière | |
| PE - Q8 - Mentions par feu (2253) | Par incendie | |
| PE - Longueurs articles x Climat (2294) | Longueur x lien CC | Articles par tranche de longueur |

## 4. Résultats

### Takeaways publiés

- Lien avec le changement climatique : 16 % des séquences audiovisuelles, 25 % des articles (contre 41 % et plus de 60 % pour la vague de chaleur de mai-juillet).
- Sécheresse citée dans 25,5 % des séquences et 32,4 % des articles.
- Écart public / privé : Arte 54 %, LCI environ 17 %, Franceinfo TV 14,7 %, BFMTV et CNews environ 8 %.
- Presse nationale 40,5 %, presse régionale 19 %.
- Secours et moyens de lutte : 78 % des séquences, 62,5 % des articles. Solutions curatives 41 % du temps d'antenne, préventives 5 %.

### Pistes sans résultat

🔴 [à compléter par l'équipe]

### Limites

Pas d'encadré méthodologique dans l'article (voir `methodo.md` §11).

### Axes à reverser dans `axes_generiques.md`

Fait.
