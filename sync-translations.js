#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const https = require("https");

// Configuration for rate limiting
const RATE_LIMIT_CONFIG = {
    initialDelay: 100,      // Initial delay in milliseconds
    maxRetries: 3,          // Maximum number of retries
    backoffMultiplier: 2,   // Exponential backoff multiplier
    maxDelay: 5000          // Maximum delay between retries
};

// Language mapping for translation
const LANGUAGE_CODES = {
    "bs": "bs",
    "es": "es",
    "fr": "fr",
    "hr": "hr",
    "hu": "hu",
    "pt-br": "pt",
    "sr": "sr",
    "zh": "zh"
};

// Explicit mapping from language codes to dictionary names
const LANGUAGE_TO_DICTIONARY = {
    "bs": "bosnian",
    "es": "spanish",
    "fr": "french",
    "hr": "croatian",
    "hu": "hungarian",
    "pt": "portuguese",
    "sr": "serbian",
    "zh": "chinese"
};

// Essential technical terms dictionary - critical HPCC/ECL terminology
const TRANSLATION_DICTIONARIES = {
    spanish: {
        "Workunit": "Unidad de Trabajo",
        "Workunits": "Unidades de Trabajo",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superarchivo",
        "Superfiles": "Superarchivos",
        "Logical File": "Archivo Lógico",
        "Logical Files": "Archivos Lógicos",
        "Query": "Consulta",
        "Queries": "Consultas",
        "Cluster": "Clúster",
        "Clusters": "Clústeres",
        "Action": "Acción",
        "Add": "Agregar",
        "Cancel": "Cancelar",
        "Close": "Cerrar",
        "Date": "Fecha",
        "Delete": "Eliminar",
        "Details": "Detalles",
        "Download": "Descargar",
        "Edit": "Editar",
        "File": "Archivo",
        "Files": "Archivos",
        "Filter": "Filtro",
        "Group": "Grupo",
        "Groups": "Grupos",
        "Name": "Nombre",
        "Open": "Abrir",
        "Owner": "Dueño",
        "Refresh": "Actualizar",
        "Save": "Guardar",
        "Search": "Buscar",
        "Size": "Tamaño",
        "State": "Estado",
        "Status": "Estado",
        "Time": "Tiempo",
        "Type": "Tipo",
        "Upload": "Subir",
        "User": "Usuario",
        "Users": "Usuarios",
        "View": "Ver"
    },
    french: {
        "Workunit": "Unité de Travail",
        "Workunits": "Unités de Travail",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superfichier",
        "Superfiles": "Superfichiers",
        "Logical File": "Fichier Logique",
        "Logical Files": "Fichiers Logiques",
        "Query": "Requête",
        "Queries": "Requêtes",
        "Cluster": "Cluster",
        "Clusters": "Clusters",
        "Action": "Action",
        "Add": "Ajouter",
        "Cancel": "Annuler",
        "Close": "Fermer",
        "Date": "Date",
        "Delete": "Surprimer",
        "Details": "Détails",
        "Download": "Télécharger",
        "Edit": "Modifier",
        "File": "Fichier",
        "Files": "Fichiers",
        "Filter": "Filtre",
        "Group": "Groupe",
        "Groups": "Groupes",
        "Name": "Nom",
        "Open": "Ouvrir",
        "Owner": "Propriétaire",
        "Refresh": "Rafraîchir",
        "Save": "Sauver",
        "Search": "Rechercher",
        "Size": "Taille",
        "State": "État",
        "Status": "État",
        "Time": "Temps",
        "Type": "Type",
        "Upload": "Télécharger",
        "User": "Utilisateur",
        "Users": "Utilisateurs",
        "View": "Voir"
    },
    portuguese: {
        "Workunit": "Unidade de Trabalho",
        "Workunits": "Unidades de Trabalho",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superarquivo",
        "Superfiles": "Superarquivos",
        "Logical File": "Arquivo Lógico",
        "Logical Files": "Arquivos Lógicos",
        "Query": "Consulta",
        "Queries": "Consultas",
        "Cluster": "Cluster",
        "Clusters": "Clusters",
        "Action": "Ação",
        "Add": "Adicionar",
        "Cancel": "Cancelar",
        "Close": "Fechar",
        "Date": "Data",
        "Delete": "Remover",
        "Details": "Detalhes",
        "Download": "Baixar",
        "Edit": "Alterar",
        "File": "Arquivo",
        "Files": "Arquivos",
        "Filter": "Filtro",
        "Group": "Grupo",
        "Groups": "Grupos",
        "Name": "Nome",
        "Open": "Abrir",
        "Owner": "Proprietário",
        "Refresh": "Recarregar",
        "Save": "Salvar",
        "Search": "Pesquisar",
        "Size": "Tamanho",
        "State": "Estado",
        "Status": "Estado",
        "Time": "Tempo",
        "Type": "Tipo",
        "Upload": "Enviar",
        "User": "Usuário",
        "Users": "Usuários",
        "View": "Visualizar"
    },
    bosnian: {
        "Workunit": "Radna Jedinica",
        "Workunits": "Radne Jedinice",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superdatoteka",
        "Superfiles": "Superdatoteke",
        "Logical File": "Logička Datoteka",
        "Logical Files": "Logičke Datoteke",
        "Query": "Upit",
        "Queries": "Upiti",
        "Cluster": "Klaster",
        "Clusters": "Klasteri",
        "Action": "Akcija",
        "Add": "Dodajte",
        "Cancel": "Poništite",
        "Close": "Zatvorite",
        "Date": "Datum",
        "Delete": "Obrišite",
        "Details": "Detalji",
        "Download": "Dobavite",
        "Edit": "Editujte",
        "File": "Datoteka",
        "Files": "Datoteke",
        "Filter": "Filter",
        "Group": "Grupa",
        "Groups": "Grupe",
        "Name": "Ime",
        "Open": "Otvoren",
        "Owner": "Vlasnik",
        "Refresh": "Osviježite",
        "Save": "Sačuvajte",
        "Search": "Pretraživanje",
        "Size": "Veličina",
        "State": "Stanje",
        "Status": "Status",
        "Time": "Vrijeme",
        "Type": "Tip",
        "Upload": "Učitajte",
        "User": "Korisnik",
        "Users": "Korisnici",
        "View": "Pogled"
    },
    croatian: {
        "Workunit": "Radna Jedinica",
        "Workunits": "Radne Jedinice",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superdatoteka",
        "Superfiles": "Superdatoteke",
        "Logical File": "Logička Datoteka",
        "Logical Files": "Logičke Datoteke",
        "Query": "Upit",
        "Queries": "Upiti",
        "Cluster": "Klaster",
        "Clusters": "Klasteri",
        "Action": "Akcija",
        "Add": "Dodajte",
        "Cancel": "Poništite",
        "Close": "Zatvorite",
        "Date": "Datum",
        "Delete": "Obrišite",
        "Details": "Detalji",
        "Download": "Dobavite",
        "Edit": "Editujte",
        "File": "Datoteka",
        "Files": "Datoteke",
        "Filter": "Filter",
        "Group": "Grupa",
        "Groups": "Grupe",
        "Name": "Ime",
        "Open": "Otvoren",
        "Owner": "Vlasnik",
        "Refresh": "Osviježite",
        "Save": "Sačuvajte",
        "Search": "Pretraživanje",
        "Size": "Veličina",
        "State": "Stanje",
        "Status": "Status",
        "Time": "Vrijeme",
        "Type": "Tip",
        "Upload": "Učitajte",
        "User": "Korisnik",
        "Users": "Korisnici",
        "View": "Pogled"
    },
    hungarian: {
        "Workunit": "Munkategység",
        "Workunits": "Munkategységek",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Szuperfájl",
        "Superfiles": "Szuperfájlok",
        "Logical File": "Logikai Fájl",
        "Logical Files": "Logikai Fájlok",
        "Query": "Lekérdezés",
        "Queries": "Lekérdezések",
        "Cluster": "Klaszter",
        "Clusters": "Klaszterek",
        "Action": "Művelet",
        "Add": "Hozzáad",
        "Cancel": "Mégsem",
        "Close": "Bezárás",
        "Date": "Dátum",
        "Delete": "Törlés",
        "Details": "Részletek",
        "Download": "Letöltés",
        "Edit": "Szerkesztés",
        "File": "Fájl",
        "Files": "Fájlok",
        "Filter": "Szűrő",
        "Group": "Csoport",
        "Groups": "Csoportok",
        "Name": "Név",
        "Open": "Megnyitás",
        "Owner": "Tulajdonos",
        "Refresh": "Frissítés",
        "Save": "Mentés",
        "Search": "Keresés",
        "Size": "Méret",
        "State": "Állapot",
        "Status": "Állapot",
        "Time": "Idő",
        "Type": "Típus",
        "Upload": "Feltöltés",
        "User": "Felhasználó",
        "Users": "Felhasználók",
        "View": "Nézet"
    },
    serbian: {
        "Workunit": "Radna Jedinica",
        "Workunits": "Radne Jedinice",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "Superdatoteka",
        "Superfiles": "Superdatoteke",
        "Logical File": "Logička Datoteka",
        "Logical Files": "Logičke Datoteke",
        "Query": "Upit",
        "Queries": "Upiti",
        "Cluster": "Klaster",
        "Clusters": "Klasteri",
        "Action": "Акција",
        "Add": "Додајте",
        "Cancel": "Поништите",
        "Close": "Затворите",
        "Date": "Датум",
        "Delete": "Обришите",
        "Details": "Детаљи",
        "Download": "Добавите",
        "Edit": "Едитујте",
        "File": "Датотека",
        "Files": "Датотеке",
        "Filter": "Филтер",
        "Group": "Група",
        "Groups": "Групе",
        "Name": "Име",
        "Open": "Отворен",
        "Owner": "Власник",
        "Refresh": "Освежите",
        "Save": "Сачувајте",
        "Search": "Претраживање",
        "Size": "Величина",
        "State": "Стање",
        "Status": "Статус",
        "Time": "Време",
        "Type": "Тип",
        "Upload": "Aплоyд",
        "User": "Корисник",
        "Users": "Корисници",
        "View": "Поглед"
    },
    chinese: {
        "Workunit": "工作单元",
        "Workunits": "工作单元",
        "ECL": "ECL",
        "WUID": "WUID",
        "HPCC": "HPCC",
        "DFU": "DFU",
        "Thor": "Thor",
        "Roxie": "Roxie",
        "Dali": "Dali",
        "ESP": "ESP",
        "Superfile": "超级文件",
        "Superfiles": "超级文件",
        "Logical File": "逻辑文件",
        "Logical Files": "逻辑文件",
        "Query": "查询",
        "Queries": "查询",
        "Cluster": "集群",
        "Clusters": "集群",
        "Action": "操作",
        "Add": "添加",
        "Cancel": "撤消",
        "Close": "关闭",
        "Date": "日期",
        "Delete": "删除",
        "Details": "细节",
        "Download": "下载",
        "Edit": "编辑",
        "File": "文件",
        "Files": "文件",
        "Filter": "过滤器",
        "Group": "组",
        "Groups": "用户组",
        "Name": "名称",
        "Open": "打开",
        "Owner": "拥有者",
        "Refresh": "更新",
        "Save": "保存",
        "Search": "搜索",
        "Size": "大小(长度)",
        "State": "状态",
        "Status": "状态",
        "Time": "时间",
        "Type": "类型",
        "Upload": "上传",
        "User": "用户",
        "Users": "用户",
        "View": "查看"
    }
};

// Free Google Translate function with retry logic
async function googleTranslate(text, targetLang, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const encodedText = encodeURIComponent(text);
        const url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=" + targetLang + "&dt=t&q=" + encodedText;

        https.get(url, (response) => {
            let data = "";

            response.on("data", (chunk) => {
                data += chunk;
            });

            response.on("end", async () => {
                // Handle rate limiting (429) or server errors (5xx)
                if (response.statusCode === 429 || response.statusCode >= 500) {
                    if (retryCount < RATE_LIMIT_CONFIG.maxRetries) {
                        const delay = Math.min(
                            RATE_LIMIT_CONFIG.initialDelay * Math.pow(RATE_LIMIT_CONFIG.backoffMultiplier, retryCount),
                            RATE_LIMIT_CONFIG.maxDelay
                        );
                        console.log("Rate limited (status " + response.statusCode + "). Retrying in " + delay + "ms (attempt " + (retryCount + 1) + "/" + RATE_LIMIT_CONFIG.maxRetries + ")...");
                        await new Promise(resolve => setTimeout(resolve, delay));
                        try {
                            const result = await googleTranslate(text, targetLang, retryCount + 1);
                            resolve(result);
                        } catch (error) {
                            reject(error);
                        }
                        return;
                    } else {
                        reject(new Error("Rate limit exceeded after " + RATE_LIMIT_CONFIG.maxRetries + " retries"));
                        return;
                    }
                }

                try {
                    const result = JSON.parse(data);
                    if (result && result[0] && result[0][0] && result[0][0][0]) {
                        resolve(result[0][0][0]);
                    } else {
                        reject(new Error("Invalid translation response"));
                    }
                } catch (error) {
                    reject(error);
                }
            });
        }).on("error", (error) => {
            reject(error);
        });
    });
}

// Enhanced translation function using dictionary + learned patterns + Google Translate
async function translateText(text, targetLang, learnedDictionary = {}) {
    // First check built-in dictionary using explicit mapping
    const dictionaryName = LANGUAGE_TO_DICTIONARY[targetLang];

    if (dictionaryName) {
        const dictionary = TRANSLATION_DICTIONARIES[dictionaryName];
        if (dictionary && dictionary[text]) {
            console.log("Using built-in dictionary for \"" + text + "\"");
            return dictionary[text];
        }
    }

    // Second, check learned dictionary from existing translations
    if (learnedDictionary[text]) {
        console.log("Using learned translation for \"" + text + "\"");
        return learnedDictionary[text];
    }

    // Use Google Translate for terms not in dictionary
    try {
        console.log("Translating \"" + text + "\" to " + targetLang + "...");
        const translation = await googleTranslate(text, targetLang);

        // Add configurable delay between successful requests to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_CONFIG.initialDelay));

        return translation;
    } catch (error) {
        console.warn("Translation failed for \"" + text + "\" to " + targetLang + ": " + error.message);
        // Fallback to manual translation marker
        return "[TRANSLATE_TO_" + targetLang.toUpperCase() + "]: " + text;
    }
}

// Build a learned dictionary from existing translations
function buildLearnedDictionary(mainKeys, existingKeys) {
    const learnedDictionary = {};
    let learnedCount = 0;

    for (const key in existingKeys) {
        if (mainKeys[key]) {
            // Map English text to translated text
            learnedDictionary[mainKeys[key]] = existingKeys[key];
            learnedCount++;
        }
    }

    console.log("  Learned " + learnedCount + " translation patterns from existing file");
    return learnedDictionary;
}

// Escape string value for TypeScript string literal
function escapeStringValue(value) {
    return value
        .replace(/\\/g, "\\\\")  // Escape backslashes first
        .replace(/"/g, '\\"')     // Escape double quotes
        .replace(/\n/g, "\\n")    // Escape newlines
        .replace(/\r/g, "\\r")    // Escape carriage returns
        .replace(/\t/g, "\\t");   // Escape tabs
}

// Extract translation keys from TypeScript export file
function extractTranslationKeys(content) {
    try {
        // Strip TypeScript export syntax and trailing semicolon
        const objectLiteral = content.replace(/^\s*export\s*=\s*/, "").replace(/;\s*$/, "").trim();

        // Validate that we have something to parse
        if (!objectLiteral || !objectLiteral.startsWith("{")) {
            throw new Error("Invalid file format: expected object literal");
        }

        // Use Function constructor to safely evaluate the object literal
        // This handles escaped quotes, newlines, and other special characters correctly
        const parsed = new Function("return " + objectLiteral)();

        // Validate the result
        if (!parsed || typeof parsed !== "object") {
            throw new Error("Parsed content is not an object");
        }

        // Handle main file format with nested 'root' property
        // Main file: export = { root: { key: "value", ... } }
        // Language files: export = { key: "value", ... }
        const keys = parsed.root || parsed;

        // Validate that we have translation keys
        if (typeof keys !== "object" || Array.isArray(keys)) {
            throw new Error("Invalid translation keys structure");
        }

        return keys;
    } catch (error) {
        console.error("Failed to parse translation file: " + error.message);
        return {};
    }
}

// Main synchronization function
async function syncTranslations(targetLanguage = null) {
    console.log("Starting translation synchronization...");

    const baseDir = path.resolve(__dirname, "./esp/src/src-dojo/nls");
    const mainFile = path.join(baseDir, "hpcc.ts");

    // Read main translation file
    if (!fs.existsSync(mainFile)) {
        console.error("Main translation file not found: " + mainFile);
        return;
    }

    const mainContent = fs.readFileSync(mainFile, "utf8");
    const mainKeys = extractTranslationKeys(mainContent);

    console.log("Found " + Object.keys(mainKeys).length + " keys in main file");

    // Filter languages if specific target is provided
    const languagesToProcess = targetLanguage
        ? Object.entries(LANGUAGE_CODES).filter(([code]) => code === targetLanguage)
        : Object.entries(LANGUAGE_CODES);

    if (targetLanguage && languagesToProcess.length === 0) {
        console.error("Language '" + targetLanguage + "' not found. Available languages: " + Object.keys(LANGUAGE_CODES).join(", "));
        return;
    }

    if (targetLanguage) {
        console.log("Processing only language: " + targetLanguage);
    }

    // Process each language directory
    for (const [langCode, langName] of languagesToProcess) {
        const langDir = path.join(baseDir, langCode);
        const langFile = path.join(langDir, "hpcc.ts");

        if (!fs.existsSync(langDir)) {
            console.log("Language directory not found: " + langCode);
            continue;
        }

        let langContent = "";
        let existingKeys = {};

        // Read existing language file if it exists
        if (fs.existsSync(langFile)) {
            langContent = fs.readFileSync(langFile, "utf8");
            existingKeys = extractTranslationKeys(langContent);
            console.log(langCode + ": Found " + Object.keys(existingKeys).length + " existing translations");
        } else {
            console.log(langCode + ": Creating new translation file");
        }

        // Build learned dictionary from existing translations
        const learnedDictionary = buildLearnedDictionary(mainKeys, existingKeys);

        // Find missing keys
        const missingKeys = Object.keys(mainKeys).filter(key => !existingKeys[key]);

        if (missingKeys.length === 0) {
            console.log(langCode + ": All translations up to date");
            continue;
        }

        console.log(langCode + ": Found " + missingKeys.length + " missing translations");

        // Generate translations for missing keys
        const newTranslations = {};
        for (const key of missingKeys) {
            const englishText = mainKeys[key];
            const translatedText = await translateText(englishText, langName, learnedDictionary);
            newTranslations[key] = translatedText;
        }

        // Update or create language file
        let updatedContent;
        if (langContent) {
            // Merge existing and new translations, then sort alphabetically (case-insensitive)
            const allTranslations = { ...existingKeys, ...newTranslations };
            const sortedEntries = Object.keys(allTranslations)
                .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
                .map(key => "    " + key + ": \"" + escapeStringValue(allTranslations[key]) + "\",")
                .join("\n");

            updatedContent = "export = {\n" + sortedEntries + "\n};\n";
        } else {
            // Create new file with all missing translations (sorted case-insensitive)
            const sortedEntries = Object.keys(newTranslations)
                .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
                .map(key => "    " + key + ": \"" + escapeStringValue(newTranslations[key]) + "\",")
                .join("\n");

            updatedContent = "export = {\n" + sortedEntries + "\n};\n";
        }

        // Write updated file
        fs.writeFileSync(langFile, updatedContent, "utf8");
        console.log(langCode + ": Added " + missingKeys.length + " new translations");
    }

    console.log("Translation synchronization complete!");
    console.log("Note: Terms marked with [TRANSLATE_TO_*] need manual translation");
}

// Run the script
if (require.main === module) {
    const targetLanguage = process.argv[2]; // Get language argument from command line
    syncTranslations(targetLanguage).catch(console.error);
}

module.exports = { syncTranslations, extractTranslationKeys };
