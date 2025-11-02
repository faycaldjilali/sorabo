import pandas as pd
import json

def filter_by_keywords(file_path, keywords):
    """Filter Excel data by keywords"""
    print("📂 Reading Excel file...")
    df = pd.read_excel(file_path)
    df_str = df.astype(str).apply(lambda x: x.str.lower())  # for case-insensitive search

    # Create one big dataframe for all results
    all_matches = pd.DataFrame()

    # Loop through each keyword
    for keyword in keywords:
        mask = df_str.apply(lambda x: x.str.contains(keyword.lower(), na=False))
        filtered_df = df[mask.any(axis=1)]
        
        if not filtered_df.empty:
            print(f"✅ Found {len(filtered_df)} rows for '{keyword}'")
            filtered_df = filtered_df.copy()
            filtered_df["keyword"] = keyword
            all_matches = pd.concat([all_matches, filtered_df], ignore_index=True)
        else:
            print(f"⚠️ No matches found for '{keyword}'.")

    return all_matches

def remove_duplicates(file_path, id_column='id'):
    """Remove duplicate rows based on ID column"""
    df = pd.read_excel(file_path)
    df_clean = df.drop_duplicates(subset=[id_column], keep='first')
    return df_clean

def get_keywords():
    """Return the list of keywords and CPV codes"""
    return [
        # Secteurs d'activité
        "miroiterie", "métallerie", "menuiserie extérieure",
        
        # CPV codes and descriptions
        "45420000", "Travaux de menuiserie et de charpenterie",
        "45421100", "Pose de portes et de fenêtres et d'éléments accessoires",
        "45421110", "Pose d'encadrements de portes et de fenêtres",
        "45421111", "Pose d'encadrements de portes",
        "45421112", "Pose d'encadrements de fenêtres",
        "45421120", "Pose de seuils",
        "45421130", "Poses de portes et de fenêtres",
        "45421131", "Pose de portes",
        "45421132", "Pose de fenêtres",
        "45421140", "Pose de menuiseries métalliques, excepté portes et fenêtres",
        "45421141", "Travaux de cloisonnement",
        "45421142", "Installation de volets",
        "45421143", "Travaux d'installation de stores",
        "45421144", "Travaux d'installation de vélums",
        "45421145", "Travaux d'installation de volets roulants",
        "44316500", "Serrurerie",
        "98395000", "Services de serrurerie",
        "44220000", "Menuiserie pour la construction",
        "45421000", "Travaux de menuiserie",
        "34928200", "Clôtures",
        "34928310", "Clôtures de protection",
        "45340000", "Travaux d'installation de clôtures, de garde-corps et de dispositifs de sécurité",
        "45342000", "Pose de clôtures",
        "42416000", "Ascenseurs, skips, monte-charges, escaliers mécaniques et trottoirs roulants",
        "42416400", "Escaliers mécaniques",
        "42419500", "Pièces pour ascenseurs, skips ou escaliers mécaniques",
        "42419530", "Pièces pour escaliers mécaniques",
        "44233000", "Escaliers",
        "44423220", "Escaliers pliants",
        "45313000", "Travaux d'installation d'ascenseurs et d'escaliers mécaniques",
        "45313200", "Travaux d'installation d'escaliers mécaniques",
        "50740000", "Services de réparation et d'entretien d'escaliers mécaniques",
        "51511000", "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques",
    ]