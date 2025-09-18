using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Globalization;
using System.IO;

class Program
{
    // ==== KONFIGURÁCIÓ ====
    static string apiKey = "00809d853c586fc7932ef7573ccb6807da92ac34";
    static string csvUrl = "https://shop.unas.hu/admin_export.php?shop_id=39377&format=unas_csv";

    // Param ID-k
    static string paramGyartoiCikkszam = "7098773";  // Gyártói cikkszám
    static string paramEAN             = "7098778";  // EAN
    static string paramSzallitasiIdo   = "7097618";  // Szállítási idő
    static string paramManufacturer    = "7391901";  // Gyártó (Márka)

    // Új termék alap adatok
    static long   newProductCategoryId = 123950; // Főkategória
    static long   subCategoryId        = 304310; // Új termékek alkategória
    static string defaultUnit          = "db";
    static string defaultVat           = "27%"; // VAT számként kell

    // Batch beállítások
    static int batchSize     = 50;
    static int batchDelayMs  = 15000;
    static bool stopOnError  = true;

    // Cache a kategóriákhoz
    static Dictionary<string,long> categoryCache = new Dictionary<string,long>();

    static async Task Main(string[] args)
    {
        try { await RunAsync(); }
        catch (Exception ex) { Console.WriteLine($"Végzetes hiba: {ex.Message}"); }
    }

    static async Task RunAsync()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("webfeltoltes/1.0 (autolife)");

        // LOGIN
        Console.WriteLine("Bejelentkezés UNAS API-ba...");
        string loginXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<Params>
  <ApiKey>{apiKey}</ApiKey>
</Params>";

        var loginResponse = await httpClient.PostAsync(
            "https://api.unas.eu/shop/login",
            new StringContent(loginXml, Encoding.UTF8, "application/xml")
        );
        string loginResponseXml = await loginResponse.Content.ReadAsStringAsync();

        if (LooksLikeError(loginResponseXml, out var loginErr))
        {
            Console.WriteLine($"Login hiba: {loginErr}");
            return;
        }

        var loginDoc = XDocument.Parse(loginResponseXml);
        string token = loginDoc.Root?.Element("Token")?.Value ?? "";
        if (string.IsNullOrEmpty(token))
        {
            Console.WriteLine("Token lekérés sikertelen (üres).");
            return;
        }
        Console.WriteLine("Token sikeresen lekérve.");

        // LÉTEZŐ TERMÉKEK LEKÉRÉSE
        var products = await GetAllProductsAsync(httpClient, token);
        Console.WriteLine($"Összesen {products.Count} termék lekérve az UNAS-ból.");

        // CSV BEOLVASÁS
        Console.WriteLine("CSV letöltése...");
        var csvBytes = await httpClient.GetByteArrayAsync(csvUrl);

        var csvDict = new Dictionary<string, CsvRow>(StringComparer.OrdinalIgnoreCase);

        using (var ms = new MemoryStream(csvBytes))
        using (var reader = new StreamReader(ms, Encoding.UTF8))
        using (var parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(reader))
        {
            parser.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
            parser.SetDelimiters(";");
            parser.HasFieldsEnclosedInQuotes = true;

            string[] headers = parser.ReadFields();
            int idxCikkszam    = FindHeader(headers, "Cikkszám");
            int idxNev         = FindHeader(headers, "Termék Név");
            int idxBrutto      = FindHeader(headers, "Bruttó Ár");
            int idxRovidLeiras = FindHeader(headers, "Rövid leírás");
            int idxTulajdonsag = FindHeader(headers, "Tulajdonságok");
            int idxKeplink     = FindHeader(headers, "Kép link");
            int idxRaktar      = FindHeader(headers, "Raktárkészlet");
            int idxTomeg       = FindHeader(headers, "Tömeg");
            int idxEAN         = FindHeader(headers, "EAN");
            int idxGyarto      = FindHeader(headers, "Gyártó");
            int idxKategoria   = FindHeader(headers, "Kategória");

            while (!parser.EndOfData)
            {
                var parts = parser.ReadFields();
                string cikkszam = SafeGet(parts, idxCikkszam);
                if (string.IsNullOrEmpty(cikkszam)) continue;

                string nevRaw   = SafeGet(parts, idxNev);
                string nev      = StripHtml(WebUtility.HtmlDecode(nevRaw));
                string brutto   = SafeGet(parts, idxBrutto);
                string ean      = SafeGet(parts, idxEAN);
                string gyarto   = SafeGet(parts, idxGyarto);
                string rovid    = WebUtility.HtmlDecode(SafeGet(parts, idxRovidLeiras));
                string hosszuleiras = WebUtility.HtmlDecode(SafeGet(parts, idxTulajdonsag));
                string kep      = SafeGet(parts, idxKeplink);
                string raktar   = SafeGet(parts, idxRaktar);
                string tomeg    = SafeGet(parts, idxTomeg);
                string kat      = SafeGet(parts, idxKategoria);

                csvDict[cikkszam] = new CsvRow
                {
                    GyartoiCikk = cikkszam,
                    Name = nev,
                    Gross = decimal.TryParse(brutto.Replace(",", "."), NumberStyles.Any, CultureInfo.InvariantCulture, out var g) ? g : 0,
                    Ean = ean,
                    Manufacturer = gyarto,
                    ShortDesc = rovid,
                    LongDesc = hosszuleiras,
                    ImageUrl = kep,
                    Stock = raktar,
                    Weight = tomeg,
                    Category = kat
                };
            }
        }

        Console.WriteLine($"\n=== Beolvasott sorok: {csvDict.Count} ===");

        // Meglévő termékek mapping
        var existingByGyCikk = new Dictionary<string, XElement>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in products)
        {
            var gy = p.Element("Params")?
                     .Elements("Param")
                     .FirstOrDefault(x => x.Element("Id")?.Value == paramGyartoiCikkszam)?
                     .Element("Value")?.Value;
            if (!string.IsNullOrWhiteSpace(gy) && !existingByGyCikk.ContainsKey(gy))
                existingByGyCikk[gy] = p;
        }

        // Frissítések összeállítása
        var updates = new List<XElement>();
        int addCount = 0, modifyCount = 0;

        foreach (var kv in csvDict)
        {
            var row = kv.Value;
            long qty = long.TryParse(row.Stock, out var q) ? q : 0;

            decimal grossVal = row.Gross;
            decimal netVal   = grossVal / 1.27m; // pontos osztás

            string grossStr = grossVal.ToString("0.##", CultureInfo.InvariantCulture);
            string netStr   = netVal.ToString("0.####", CultureInfo.InvariantCulture);

            string szallitasiIdo = qty == 0 ? "Érdeklődjön" : "2-3 munkanap";
            string empty         = qty == 0 ? "0" : "1";

            if (existingByGyCikk.TryGetValue(row.GyartoiCikk, out var prod))
            {
                // MODIFY
                string sku = prod.Element("Sku")?.Value ?? "";
                if (string.IsNullOrEmpty(sku)) continue;

                var productXml = new XElement("Product",
                    new XElement("Sku", sku),
                    new XElement("Action", "modify"),
                    new XElement("Params",
                        new XElement("Param",
                            new XElement("Id", paramSzallitasiIdo),
                            new XElement("Value", new XCData(szallitasiIdo))
                        )
                    ),
                    new XElement("Stocks",
                        new XElement("Stock", new XElement("Qty", qty)),
                        new XElement("Status", new XElement("Empty", empty))
                    ),
                    new XElement("Prices",
                        new XElement("Vat", defaultVat),
                        new XElement("Price",
                            new XElement("Type", "normal"),
                            new XElement("Net", netStr),
                            new XElement("Gross", grossStr)
                        )
                    )
                );

                // Súly
                if (!string.IsNullOrWhiteSpace(row.Weight))
                {
                    if (decimal.TryParse(row.Weight.Replace(",", "."), NumberStyles.Any, CultureInfo.InvariantCulture, out var w))
                    {
                        productXml.Add(new XElement("Weight", w.ToString(CultureInfo.InvariantCulture)));
                    }
                }

                updates.Add(productXml);
                modifyCount++;
            }
            else
            {
                // ÚJ TERMÉK ADD
                string cleanedSku = SanitizeSku(row.GyartoiCikk) + "_al";
                if (string.IsNullOrWhiteSpace(cleanedSku))
                    cleanedSku = "SKU_" + Guid.NewGuid().ToString("N").Substring(0, 10);

                string name = string.IsNullOrWhiteSpace(row.Name) ? $"Új termék {row.GyartoiCikk}" : row.Name;

                var productXml = new XElement("Product",
                    new XElement("Action", "add"),
                    new XElement("Statuses",
                        new XElement("Status",
                            new XElement("Type", "base"),
                            new XElement("Value", "0")
                        )
                    ),
                    new XElement("Sku", cleanedSku),
                    new XElement("Name", name),
                    new XElement("Unit", defaultUnit),
                    new XElement("Categories",
                        new XElement("Category",
                            new XElement("Id", subCategoryId),
                            new XElement("Type", "base")
                        )
                    ),
                    new XElement("Params",
                        new XElement("Param",
                            new XElement("Id", paramGyartoiCikkszam),
                            new XElement("Value", new XCData(row.GyartoiCikk))
                        ),
                        new XElement("Param",
                            new XElement("Id", paramSzallitasiIdo),
                            new XElement("Value", new XCData(szallitasiIdo))
                        )
                    ),
                    new XElement("Stocks",
                        new XElement("Stock", new XElement("Qty", qty)),
                        new XElement("Status", new XElement("Empty", empty))
                    ),
                    new XElement("Prices",
                        new XElement("Vat", defaultVat),
                        new XElement("Price",
                            new XElement("Type", "normal"),
                            new XElement("Net", netStr),
                            new XElement("Gross", grossStr)
                        )
                    )
                );

                // Súly
                if (!string.IsNullOrWhiteSpace(row.Weight))
                {
                    if (decimal.TryParse(row.Weight.Replace(",", "."), NumberStyles.Any, CultureInfo.InvariantCulture, out var w) && w > 0)
                    {
                        productXml.Add(new XElement("Weight", w.ToString(CultureInfo.InvariantCulture)));
                    }
                }

                // Rövid / hosszú leírás
                if (!string.IsNullOrWhiteSpace(row.ShortDesc) || !string.IsNullOrWhiteSpace(row.LongDesc))
                {
                    var desc = new XElement("Description");
                    if (!string.IsNullOrWhiteSpace(row.ShortDesc))
                        desc.Add(new XElement("Short", new XCData(row.ShortDesc)));
                    if (!string.IsNullOrWhiteSpace(row.LongDesc))
                        desc.Add(new XElement("Long", new XCData(row.LongDesc)));
                    productXml.Add(desc);
                }

                // EAN
                if (!string.IsNullOrWhiteSpace(row.Ean))
                {
                    productXml.Element("Params")!.Add(
                        new XElement("Param",
                            new XElement("Id", paramEAN),
                            new XElement("Value", new XCData(row.Ean))
                        )
                    );
                }

                // Gyártó
                if (!string.IsNullOrWhiteSpace(row.Manufacturer))
                {
                    productXml.Element("Params")!.Add(
                        new XElement("Param",
                            new XElement("Id", paramManufacturer),
                            new XElement("Value", new XCData(row.Manufacturer))
                        )
                    );
                }

                // ✅ Kategória útvonal létrehozása
                long parentId = subCategoryId;
                if (!string.IsNullOrWhiteSpace(row.Category))
                {
                    var levels = row.Category.Split('|', StringSplitOptions.RemoveEmptyEntries)
                                             .Select(x => x.Trim()).ToList();

                    foreach (var level in levels)
                    {
                        long catId = EnsureCategoryExists(httpClient, token, parentId, level).Result;
                        parentId = catId;
                    }

                    productXml.Element("Categories")!.Add(
                        new XElement("Category",
                            new XElement("Id", parentId),
                            new XElement("Type", "alt")
                        )
                    );
                }

                // Képek
                if (!string.IsNullOrWhiteSpace(row.ImageUrl))
                {
                    var images = new XElement("Images");
                    var urls = row.ImageUrl.Split('|', StringSplitOptions.RemoveEmptyEntries);

                    for (int i = 0; i < urls.Length; i++)
                    {
                        string url = urls[i].Trim();
                        if (string.IsNullOrEmpty(url)) continue;

                        var img = new XElement("Image",
                            new XElement("Type", i == 0 ? "base" : "alt"),
                            new XElement("Import",
                                new XElement("Url", url)
                            ),
                            new XElement("Filename", new XCData(Path.GetFileNameWithoutExtension(url))),
                            new XElement("Alt", new XCData(row.Name))
                        );

                        if (i > 0)
                            img.Add(new XElement("Id", i));

                        images.Add(img);
                    }

                    productXml.Add(images);
                }

                updates.Add(productXml);
                addCount++;
            }
        }

        // === Batch küldés ===
        int batchNo = 0;
        for (int i = 0; i < updates.Count; i += batchSize)
        {
            var batch = updates.Skip(i).Take(batchSize).ToList();
            var updateXml = new XDocument(
                new XDeclaration("1.0", "UTF-8", null),
                new XElement("Products", batch)
            );

            batchNo++;
            Console.WriteLine($"\n=== KÜLDÖTT XML (batch {batchNo}, {batch.Count} tétel) ===");
            Console.WriteLine(updateXml.ToString());

            var setReq = new HttpRequestMessage(HttpMethod.Post, "https://api.unas.eu/shop/setProduct");
            setReq.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            setReq.Content = new StringContent(updateXml.ToString(), Encoding.UTF8, "application/xml");

            HttpResponseMessage setResp;
            string setRespText;
            try
            {
                setResp = await httpClient.SendAsync(setReq);
                setRespText = await setResp.Content.ReadAsStringAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Hálózati hiba (batch {batchNo}): {ex.Message}");
                if (stopOnError) return;
                await Task.Delay(batchDelayMs);
                continue;
            }

            Console.WriteLine("\n=== Frissítés válasz ===");
            Console.WriteLine(setRespText);

            if (LooksLikeError(setRespText, out var errMessage))
            {
                Console.WriteLine($"UNAS hiba: {errMessage}");
                if (stopOnError) return;
            }

            await Task.Delay(batchDelayMs);
        }

        // Összegzés
        Console.WriteLine($"\n=== ÖSSZEGZÉS ===");
        Console.WriteLine($"CSV sorok (érvényes): {csvDict.Count}");
        Console.WriteLine($"Küldött tételek:      {updates.Count}");
        Console.WriteLine($" - MODIFY:            {modifyCount}");
        Console.WriteLine($" - ADD:               {addCount}");
    }

    // ====== Kategória létrehozás ======
    static async Task<long> EnsureCategoryExists(HttpClient httpClient, string token, long parentId, string name)
    {
        string key = parentId + "|" + name.ToLowerInvariant();
        if (categoryCache.TryGetValue(key, out var existingId))
            return existingId;

        // Kategóriák lekérése
        string getCatXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<Params>
  <LimitNum>1000</LimitNum>
</Params>";

        var req = new HttpRequestMessage(HttpMethod.Post, "https://api.unas.eu/shop/getCategory");
        req.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        req.Content = new StringContent(getCatXml, Encoding.UTF8, "application/xml");

        var resp = await httpClient.SendAsync(req);
        string xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);

        var match = doc.Descendants("Category")
                       .FirstOrDefault(c => string.Equals(c.Element("Name")?.Value, name, StringComparison.OrdinalIgnoreCase)
                                         && c.Element("Parent")?.Element("Id")?.Value == parentId.ToString());

        if (match != null)
        {
            long id = long.Parse(match.Element("Id")!.Value);
            categoryCache[key] = id;
            return id;
        }

        // Ha nincs → létrehozzuk
        string sef = RemoveAccents(name)
    .Replace(" ", "-")
    .Replace("&", "es")   // <-- új: az & karaktert cseréli
    .ToLowerInvariant();
        var newCatXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<Categories>
  <Category>
    <Action>add</Action>
    <Parent>
      <Id>{parentId}</Id>
    </Parent>
    <Name><![CDATA[{name}]]></Name>
    <SefUrl><![CDATA[{sef}]]></SefUrl>
  </Category>
</Categories>";

        var addReq = new HttpRequestMessage(HttpMethod.Post, "https://api.unas.eu/shop/setCategory");
        addReq.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        addReq.Content = new StringContent(newCatXml, Encoding.UTF8, "application/xml");

        var addResp = await httpClient.SendAsync(addReq);
        string addXml = await addResp.Content.ReadAsStringAsync();
        var addDoc = XDocument.Parse(addXml);
        var idElem = addDoc.Descendants("Id").FirstOrDefault();

        if (idElem != null && long.TryParse(idElem.Value, out var newId))
        {
            categoryCache[key] = newId;
            return newId;
        }

        throw new Exception($"Nem sikerült létrehozni kategóriát: {name}");
    }

    // ====== UNAS lekérés lapozással ======
    static async Task<List<XElement>> GetAllProductsAsync(HttpClient httpClient, string token)
    {
        var all = new List<XElement>();
        int pageSize = 1000;
        foreach (var status in new[] { 0, 1, 2 })
        {
            int start = 0;
            while (true)
            {
                string getProductXml = $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<Params>
  <StatusBase>{status}</StatusBase>
  <LimitNum>{pageSize}</LimitNum>
  <LimitStart>{start}</LimitStart>
  <ContentType>full</ContentType>
</Params>";

                var req = new HttpRequestMessage(HttpMethod.Post, "https://api.unas.eu/shop/getProduct");
                req.Headers.Add("Authorization", $"Bearer {token}");
                req.Content = new StringContent(getProductXml, Encoding.UTF8, "application/xml");

                var resp = await httpClient.SendAsync(req);
                string xml = await resp.Content.ReadAsStringAsync();

                if (LooksLikeError(xml, out var getErr))
                {
                    Console.WriteLine($"getProduct hiba: {getErr}");
                    break;
                }

                var doc = XDocument.Parse(xml);
                var page = doc.Descendants("Product").ToList();

                if (page.Count == 0) break;
                all.AddRange(page);

                if (page.Count < pageSize) break;
                start += pageSize;
                await Task.Delay(300);
            }
        }

        return all;
    }

    // ====== segédek ======
    static string SafeGet(string[] arr, int idx) => (idx >= 0 && idx < arr.Length) ? arr[idx].Trim().Trim('"') : "";

    static bool LooksLikeError(string xmlText, out string message)
    {
        message = "";
        try
        {
            var doc = XDocument.Parse(xmlText);
            var err = doc.Root;
            if (err != null && err.Name.LocalName.Equals("Error", StringComparison.OrdinalIgnoreCase))
            {
                message = err.Value?.Trim() ?? "Ismeretlen hiba";
                return true;
            }
        }
        catch { }
        return false;
    }

    struct CsvRow
    {
        public string GyartoiCikk;
        public string Name;
        public decimal Gross;
        public string Ean;
        public string Manufacturer;
        public string ShortDesc;
        public string LongDesc;
        public string ImageUrl;
        public string Stock;
        public string Weight;
        public string Category;
    }

    static int FindHeader(string[] header, string name)
        => Array.FindIndex(header, h => h.Trim().Trim('"').Contains(name, StringComparison.OrdinalIgnoreCase));

    static string SanitizeSku(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        string s = RemoveAccents(raw);
        var sb = new StringBuilder(s.Length);
        foreach (char c in s)
        {
            char ch = c;
            if (char.IsWhiteSpace(ch)) ch = '-';
            if (IsAllowedSkuChar(ch)) sb.Append(ch);
        }
        string cleaned = System.Text.RegularExpressions.Regex.Replace(sb.ToString(), "-{2,}", "-").Trim('-');
        if (cleaned.Length > 64) cleaned = cleaned.Substring(0, 64);
        return cleaned;
    }

    static bool IsAllowedSkuChar(char c)
        => (c >= 'A' && c <= 'Z')
        || (c >= 'a' && c <= 'z')
        || (c >= '0' && c <= '9')
        || c == '-' || c == '_';

    static string RemoveAccents(string input)
    {
        var repl = new Dictionary<char, string>
        {
            ['á'] = "a", ['é'] = "e", ['í'] = "i", ['ó'] = "o", ['ö'] = "o", ['ő'] = "o",
            ['ú'] = "u", ['ü'] = "u", ['ű'] = "u", ['Á'] = "A", ['É'] = "E", ['Í'] = "I",
            ['Ó'] = "O", ['Ö'] = "O", ['Ő'] = "O", ['Ú'] = "U", ['Ü'] = "U", ['Ű'] = "U",
            ['ß'] = "ss"
        };
        var sb = new StringBuilder(input.Length);
        foreach (var ch in input.Normalize(NormalizationForm.FormD))
        {
            var uc = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (repl.TryGetValue(ch, out var rep)) { sb.Append(rep); continue; }
            if (uc != UnicodeCategory.NonSpacingMark) sb.Append(ch);
        }
        return sb.ToString().Normalize(NormalizationForm.FormC);
    }

    static string StripHtml(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "";
        return System.Text.RegularExpressions.Regex.Replace(input, "<.*?>", "").Trim();
    }
}
