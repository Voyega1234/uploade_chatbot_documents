// Complete KOL Discovery System for Google Apps Script
// Main Logic File - Code.gs (Updated for Multi-User Support) - FIXED VERSION

// Configuration
const CONFIG = {
  OPENAI_API_KEY: PropertiesService.getScriptProperties().getProperty('OPENAI_API_KEY'),
  APIFY_API_TOKEN: PropertiesService.getScriptProperties().getProperty('APIFY_API_TOKEN') ,
  APIFY_ACTOR_ID: 'GdWCkxBtKWOsKjdch',
  TIKTOK_ACCESS_TOKEN: PropertiesService.getScriptProperties().getProperty('TIKTOK_ACCESS_TOKEN'),
  TTO_TCM_ACCOUNT_ID: PropertiesService.getScriptProperties().getProperty('TTO_TCM_ACCOUNT_ID'),
  OPENAI_MODEL: 'gpt-4o-mini'
};

const SCOUTING_SHEET_NAME = 'Scouting';

// Follower tier definitions
const FOLLOWER_TIERS = {
  'Below': { min: 0, max: 999 },
  'Nano': { min: 1000, max: 9999 },
  'Micro': { min: 10000, max: 99999 },
  'Mid': { min: 100000, max: 999999 },
  'Macro': { min: 1000000, max: 9999999 },
  'Mega': { min: 10000000, max: 999999999 }
};

// ==================== SESSION MANAGEMENT ====================

/**
 * Ensure Scouting sheet is available and return sheet info
 */
function createNewSession() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(SCOUTING_SHEET_NAME);
    if (!sheet) {
      throw new Error('Sheet "Scouting" not found. Please create it first.');
    }

    const userProps = PropertiesService.getUserProperties();
    userProps.setProperty('CURRENT_SESSION_ID', SCOUTING_SHEET_NAME);
    userProps.setProperty('CURRENT_SHEET_NAME', SCOUTING_SHEET_NAME);

    return {
      success: true,
      sessionId: SCOUTING_SHEET_NAME,
      sheetName: SCOUTING_SHEET_NAME,
      message: `Ready to use sheet: ${SCOUTING_SHEET_NAME}`
    };
  } catch (error) {
    console.error('Error preparing session:', error);
    return {
      success: false,
      message: 'Unable to use the Scouting sheet: ' + error.message
    };
  }
}

/**
 * Get current session info
 */
function getCurrentSession() {
  return {
    sessionId: SCOUTING_SHEET_NAME,
    sheetName: SCOUTING_SHEET_NAME
  };
}

/**
 * Get current session's sheet
 */
function getCurrentSessionSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SCOUTING_SHEET_NAME);

  if (!sheet) {
    throw new Error('Scouting sheet not found');
  }

  return sheet;
}

/**
 * Switch to current session sheet
 */
function switchToCurrentSessionSheet() {
  try {
    const sheet = getCurrentSessionSheet();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    ss.setActiveSheet(sheet);
    
    return {
      success: true,
      sheetName: sheet.getName(),
      message: 'Switched to sheet ' + sheet.getName() + ' successfully'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Unable to switch sheet: ' + error.message
    };
  }
}

// ==================== UPDATED CORE FUNCTIONS ====================

function saveBriefToSheet(briefText) {
  try {
    const sessionInfo = createNewSession();
    if (!sessionInfo.success) {
      throw new Error(sessionInfo.message);
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const briefSheet = ss.getSheetByName('Brief_KOL');
    if (briefSheet) {
      briefSheet.getRange('A2').setValue(briefText);
    }

    switchToCurrentSessionSheet();

    return {
      success: true,
      message: 'Saved successfully: using sheet ' + sessionInfo.sheetName,
      sessionId: sessionInfo.sessionId,
      sheetName: sessionInfo.sheetName
    };

  } catch (e) {
    Logger.log(e.message);
    throw e;
  }
}

/**
 * Check if there's KOL data in ACTIVE sheet (not session-based)
 */
function hasKOLData() {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const lastRow = sheet.getLastRow();
    
    Logger.log('Last row: ' + lastRow);
    
    if (lastRow < 2) {
      Logger.log('No data rows (lastRow < 2)');
      return false;
    }
    
    for (let row = 2; row <= lastRow; row++) {
      const valueB = sheet.getRange(row, 2).getValue();
      const valueC = sheet.getRange(row, 3).getValue();
      
      Logger.log(`Row ${row}: B="${valueB}", C="${valueC}"`);
      
      if ((valueB && valueB.toString().trim() !== '') || 
          (valueC && valueC.toString().trim() !== '')) {
        Logger.log(`Row ${row}: Has data - returning true`);
        return true;
      }
    }
    
    Logger.log('No data found in any row');
    return false;
    
  } catch (error) {
    Logger.log('Error in hasKOLData: ' + error.message);
    console.error('Error checking KOL data:', error);
    return false;
  }
}

/**
 * Setup auto-score trigger - works with ACTIVE sheet
 */
function setupAutoScoreTrigger() {
  try {
    const activeSheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const functionName = 'processMultiplePromptsUnified';
    
    // Delete existing triggers for this function
    const triggers = ScriptApp.getProjectTriggers();
    for (let trigger of triggers) {
      if (trigger.getHandlerFunction() === functionName) {
        ScriptApp.deleteTrigger(trigger);
      }
    }
    
    // Create new trigger
    ScriptApp.newTrigger(functionName)
      .timeBased()
      .everyMinutes(5)
      .create();
    
    Logger.log('Auto-score trigger created successfully for sheet: ' + activeSheet.getName());
    return { 
      success: true, 
      message: `Auto-score scheduled every 5 minutes (Sheet: ${activeSheet.getName()})` 
    };
  } catch (error) {
    console.error('Error setting up trigger:', error);
    return { success: false, message: error.message };
  }
}

function processMultiplePromptsUnified() {
  // ======= CONFIGURATION =======
  const GEMINI_MODEL = 'gemini-2.5-flash';
  const TEMPERATURE = 0.2;
  const MAX_OUTPUT_TOKENS = 30000;
  const MAX_API_RETRIES = 2;
  let ENABLE_GROUNDING = true;
  const GROUNDING_THRESHOLD = 0.7;
  const THROTTLE_MS = 800;
  const MAX_EXECUTION_TIME = 4.5 * 60 * 1000;
  const BATCH_SIZE = 5;
  const STOP_AFTER_CONSECUTIVE_SKIPS = 10;
  
  const startTime = new Date().getTime();

  // Column definitions
  const START_ROW = 2;
  const PROMPT_COLS = [16, 17, 18]; // P, Q, R
  const OUTPUT_COLS = [13, 14, 15]; // M, N, O
  const COL_NAMES = ['Prompt: Account Relevance', 'Prompt: Content Quality', 'Prompt: Brand Safety'];
  const DEPENDENCY_COLS = [2, 3, 4, 5]; // B, C, D, E

  const ss = SpreadsheetApp.getActive();
  const properties = PropertiesService.getScriptProperties();

  // ======= HELPER FUNCTIONS =======
  function deleteAllTriggers() {
    const triggers = ScriptApp.getProjectTriggers();
    let deletedCount = 0;
    triggers.forEach(trigger => {
      try {
        ScriptApp.deleteTrigger(trigger);
        deletedCount++;
      } catch (err) {
        Logger.log(`Error deleting trigger: ${err.message}`);
      }
    });
    return deletedCount;
  }

  function hasRemainingWork(sheet, startRow, lastRow) {
    for (let row = startRow; row <= lastRow; row++) {
      const existingOutputs = OUTPUT_COLS.map(col => String(sheet.getRange(row, col).getValue()).trim());
      if (existingOutputs.some(output => output === '')) {
        const prompts = PROMPT_COLS.map(col => String(sheet.getRange(row, col).getValue()).trim());
        const hasValidPrompt = prompts.some(p => p && p.toUpperCase() !== 'N/A' && !p.includes('{{') && p !== '#ERROR!');
        if (hasValidPrompt) return true;
      }
    }
    return false;
  }

  function getApiKey() {
    const key = properties.getProperty('Admin-Gemini-KOL Specialist');
    if (!key || !key.trim()) {
      throw new Error("Missing API key. Set Script Property 'Admin-Gemini-KOL Specialist'");
    }
    return key.trim();
  }

  function waitForFormulasToCalculate(sheet, row, maxRetries = 10) {
    for (let retries = 0; retries < maxRetries; retries++) {
        const promptValues = PROMPT_COLS.map(col => String(sheet.getRange(row, col).getValue()).trim());
        const hasValidPrompts = promptValues.some(val => val && val.toUpperCase() !== 'N/A' && !val.includes('{{') && val !== '#ERROR!');
        if (hasValidPrompts || promptValues.every(val => val.toUpperCase() === 'N/A')) {
          return true;
        }
        if (retries < maxRetries - 1) {
          Utilities.sleep(1000);
          SpreadsheetApp.flush();
        }
    }
    Logger.log(`Row ${row}: WARNING - Formulas might not be fully calculated.`);
    return false;
  }

  function formatGroundingMetadata(groundingMetadata) {
    if (!groundingMetadata) return '';
    let out = '';
    if (groundingMetadata.webSearchQueries?.length) {
      out += '\n--- Search Queries ---\n' + groundingMetadata.webSearchQueries.join('\n');
    }
    if (groundingMetadata.groundingChunks?.length) {
      out += '\n--- Sources ---\n';
      groundingMetadata.groundingChunks.forEach((chunk, i) => {
        if (chunk.web?.uri) {
          out += `${i + 1}. ${chunk.web.title || ''} - ${chunk.web.uri}\n`;
        }
      });
    }
    return out;
  }

  function generateWithGemini(userPrompt) {
    const API_KEY = getApiKey();
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${API_KEY}`;
    const body = {
      contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
      generationConfig: {
        temperature: TEMPERATURE,
        maxOutputTokens: MAX_OUTPUT_TOKENS
      }
    };
    if (ENABLE_GROUNDING) {
      body.tools = [{ google_search: {} }];
    }
    const res = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    const txt = res.getContentText();
    if (code !== 200) throw new Error(`HTTP ${code}: ${txt}`);
    const data = JSON.parse(txt);
    if (!data.candidates || !data.candidates.length) throw new Error('No candidates returned.');
    const cand = data.candidates[0];
    if (cand.finishReason && cand.finishReason !== 'STOP') {
      return `BLOCKED: ${cand.finishReason}`;
    }
    let answer = ((cand.content && cand.content.parts) || []).map(p => p.text || '').join('').trim();
    if (ENABLE_GROUNDING && cand.groundingMetadata) {
      answer += '\n\n' + formatGroundingMetadata(cand.groundingMetadata);
    }
    return answer || 'NO_CONTENT_GENERATED';
  }

  function buildCombinedPrompt(prompts, colNames, isRetry = false) {
    let combinedPrompt = "Please analyze the following and provide responses for each section:\n\n";
    prompts.forEach((prompt, i) => {
      if (prompt && prompt.toUpperCase() !== 'N/A' && prompt !== 'undefined' && prompt !== 'null' && !prompt.includes('{{')) {
        combinedPrompt += `**${colNames[i]} Analysis:**\n${prompt}\n\n`;
      }
    });
    combinedPrompt += "Please provide your response in the following format:\n";
    combinedPrompt += "=== ACCOUNT_RELEVANCE ===\n[Your analysis for Account Relevance]\n\n";
    combinedPrompt += "=== CONTENT_QUALITY ===\n[Your analysis for Content Quality]\n\n";
    combinedPrompt += "=== BRAND_SAFETY ===\n[Your analysis for Brand Safety]\n\n";
    combinedPrompt += "If a section did not have a prompt, respond with 'N/A' for that section.";
    if (isRetry) {
      combinedPrompt += "\n\n**IMPORTANT INSTRUCTION: Your previous response was too long and was cut off. Please provide a more CONCISE and BRIEF summary for each analysis section to ensure the full response fits.**";
    }
    return combinedPrompt;
  }

  function parseMultiResponse(response) {
    const result = { account_relevance: 'N/A', content_quality: 'N/A', brand_safety: 'N/A' };
    const sections = response.split(/===\s*(ACCOUNT_RELEVANCE|CONTENT_QUALITY|BRAND_SAFETY)\s*===/i);
    if (sections.length > 1) {
      for (let i = 1; i < sections.length; i += 2) {
        const sectionName = sections[i].toLowerCase().trim();
        const sectionContent = (sections[i + 1] || '').trim();
        if (sectionName.includes('account_relevance')) result.account_relevance = sectionContent;
        else if (sectionName.includes('content_quality')) result.content_quality = sectionContent;
        else if (sectionName.includes('brand_safety')) result.brand_safety = sectionContent;
      }
    } else {
      result.account_relevance = response;
    }
    return result;
  }

  function performCleanupAndStop(message) {
    Logger.log('Step 1: Resetting resume state...');
    properties.deleteProperty('multi_gemini_resume_state');
    Logger.log('Step 2: Deleting all project triggers...');
    const deletedCount = deleteAllTriggers();
    const session = getCurrentSession();
    const finalMessage = `${message} | Sheet: ${session.sheetName} | Resume state cleared. ${deletedCount} trigger(s) removed.`;
    ss.toast(finalMessage, '✅ Cleanup Complete', 10);
  }
  
  // ======= MAIN EXECUTION =======
  try {
    getApiKey();
  } catch (err) {
    ss.toast(err.message, 'Error', 5);
    return;
  }

  let sh;
  try {
    sh = getCurrentSessionSheet(); // Use current session sheet instead of fixed name
    if (!sh) throw new Error(`Current session sheet not found`);
  } catch (err) {
    ss.toast(err.message, 'Sheet Error', 5);
    return;
  }

  const lastRow = sh.getLastRow();
  if (lastRow < START_ROW) {
    performCleanupAndStop('No data to process.');
    return;
  }

  if (!hasRemainingWork(sh, START_ROW, lastRow)) {
    performCleanupAndStop('No remaining work.');
    return;
  }

  const startRowOffset = 0;
  const numRows = lastRow - START_ROW + 1;
  let totalProcessed = 0;
  let consecutiveSkips = 0;

  properties.deleteProperty('multi_gemini_resume_state');

  ss.toast('Starting multi-prompt processing from beginning...', 'Processing', 3);

  for (let batchStart = startRowOffset; batchStart < numRows; batchStart += BATCH_SIZE) {
    const currentTime = new Date().getTime();
    if (currentTime - startTime > MAX_EXECUTION_TIME) {
      ss.toast(`Timeout reached. Processing stopped. Please run again to continue.`, 'Timeout', 8);
      return;
    }

    const batchEnd = Math.min(batchStart + BATCH_SIZE, numRows);
    for (let i = batchStart; i < batchEnd; i++) {
      const actualRowNum = START_ROW + i;
      waitForFormulasToCalculate(sh, actualRowNum);
      
      try {
          const rangeA_K = sh.getRange(actualRowNum, 1, 1, 11).getValues()[0];
          const isRowEmptyA_K = rangeA_K.every(cell => !String(cell).trim());

          if (isRowEmptyA_K) {
              Logger.log(`Row ${actualRowNum}: SKIPPING - Columns A-K are entirely empty.`);
              consecutiveSkips++;
              if (consecutiveSkips >= STOP_AFTER_CONSECUTIVE_SKIPS) {
                  const stopMsg = `Stopped: Found ${consecutiveSkips} consecutive entirely empty rows.`;
                  performCleanupAndStop(stopMsg);
                  return;
              }
              continue;
          }
      } catch (e) {
          Logger.log(`Error checking A-K for row ${actualRowNum}: ${e.message}. Treating as empty and skipping.`);
          consecutiveSkips++;
          if (consecutiveSkips >= STOP_AFTER_CONSECUTIVE_SKIPS) {
              const stopMsg = `Stopped: Found ${consecutiveSkips} consecutive empty/error rows.`;
              performCleanupAndStop(stopMsg);
              return;
          }
          continue;
      }

      const existingOutputs = OUTPUT_COLS.map(col => String(sh.getRange(actualRowNum, col).getValue()).trim());
      
      const prompts = PROMPT_COLS.map(col => String(sh.getRange(actualRowNum, col).getValue()).trim());
      const validPrompts = prompts.filter(p => p && p.toUpperCase() !== 'N/A' && !p.includes('{{') && p !== '#ERROR!');
      const hasAnyPrompt = validPrompts.length > 0;

      const hasEmptyOutput = existingOutputs.some(output => output === '');

      if (!hasAnyPrompt || !hasEmptyOutput) {
        consecutiveSkips++;
        if (consecutiveSkips >= STOP_AFTER_CONSECUTIVE_SKIPS) {
          const stopMsg = `Stopped: Found ${consecutiveSkips} consecutive empty/processed rows.`;
          performCleanupAndStop(stopMsg);
          return;
        }
        continue;
      }

      consecutiveSkips = 0;

      try {
        let response = '';
        let success = false;
        for (let attempt = 1; attempt <= MAX_API_RETRIES; attempt++) {
          Logger.log(`Row ${actualRowNum}: API call attempt ${attempt}/${MAX_API_RETRIES}`);
          const isRetryAttempt = (attempt > 1);
          const combinedPrompt = buildCombinedPrompt(prompts, COL_NAMES, isRetryAttempt);
          response = generateWithGemini(combinedPrompt);
          if (response.includes('BLOCKED: MAX_TOKENS')) {
            Logger.log(`Row ${actualRowNum}: Attempt ${attempt} failed with MAX_TOKENS. Retrying...`);
            Utilities.sleep(1000);
          } else {
            success = true;
            break;
          }
        }
        if (!success) {
          throw new Error(response);
        }

        const parsedResponses = parseMultiResponse(response);
        const responses = [parsedResponses.account_relevance, parsedResponses.content_quality, parsedResponses.brand_safety];
        
        OUTPUT_COLS.forEach((col, j) => {
          if (!existingOutputs[j]) {
            sh.getRange(actualRowNum, col).setValue(responses[j]);
          }
        });
        totalProcessed++;
      } catch (err) {
        const errorMsg = `ERROR: ${err.message}`;
        OUTPUT_COLS.forEach((col, j) => {
          if (!existingOutputs[j]) {
            sh.getRange(actualRowNum, col).setValue(errorMsg);
          }
        });
        Logger.log(`Row ${actualRowNum}: ${errorMsg}`);
      }
      if (THROTTLE_MS > 0) Utilities.sleep(THROTTLE_MS);
    }
    SpreadsheetApp.flush();
  }

  const totalTime = Math.round((new Date().getTime() - startTime) / 1000);
  if (!hasRemainingWork(sh, START_ROW, lastRow)) {
    const successMsg = `All processing complete! ${totalProcessed} rows processed in ${totalTime}s.`;
    performCleanupAndStop(successMsg);
  }
}

// ==================== UPDATED UTILITY FUNCTIONS ====================

function getKOLScoutingSheet() {
  return getCurrentSessionSheet();
}

function checkAndRedirectToKOLSheet() {
  try {
    const result = switchToCurrentSessionSheet();
    return result;
  } catch (error) {
    return {
      redirected: false,
      message: 'Error: ' + error.message
    };
  }
}

/**
 * Updated clear functions to work with ACTIVE sheet
 */
function clearColumnsAtoKandMtoO() {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const lastRow = sheet.getLastRow();

    if (lastRow <= 1) {
    return { success: true, message: 'No data to clear on sheet ' + sheet.getName() };
    }

    // Clear columns A-K (1-11)
    const rangeAtoK = sheet.getRange(2, 1, lastRow - 1, 11);
    rangeAtoK.clearContent();

    // Clear columns M-O (13-15)
    const rangeMtoO = sheet.getRange(2, 13, lastRow - 1, 3);
    rangeMtoO.clearContent();

    return { success: true, message: 'Cleared data on sheet ' + sheet.getName() + ' successfully' };
  } catch (error) {
    return { success: false, message: 'Error: ' + error.message };
  }
}

/**
 * Updated append function to work with SESSION sheet (from getCurrentSessionSheet)
 */
function appendToSheet(kols) {
  const sheet = getCurrentSessionSheet(); // Still relies on the session sheet because searchKOLsWithKeywords calls it
  
  // Find last row with data in columns A-K only
  let lastRowWithData = 0;
  
  for (let row = 1; row <= 1000; row++) {
    const range = sheet.getRange(row, 1, 1, 11); // A-K
    const values = range.getValues()[0];
    
    const hasData = values.some(cell => cell !== null && cell !== undefined && cell !== '');
    
    if (hasData) {
      lastRowWithData = row;
    }
  }
  
  let startRow = lastRowWithData + 1;
  
  if (lastRowWithData <= 1) {
    startRow = 2; // Start after header
  }
  
  // Create data rows
  const rows = [];
  kols.forEach((kol, index) => {
    const engagementRate = kol.engagementRate < 1 ? kol.engagementRate * 100 : kol.engagementRate;
    const rowNumber = startRow + index; // No need to subtract 1; write sequentially
    
    rows.push([
      rowNumber,
      kol.profileUrl || '',
      kol.handleName || '',
      kol.nickname || '',
      (kol.bio || '').substring(0, 200),
      kol.followersCount || 0,
      Math.round(engagementRate * 100) / 100,
      kol.creatorPrice || 'None',
      kol.tiktokRegistered || 'Unknown',
      kol.source || 'Unknown',
      kol.sourceKeyword || ''
    ]);
  });
  
  if (rows.length > 0) {
    const dataRange = sheet.getRange(startRow, 1, rows.length, 11);
    dataRange.setValues(rows);
    
    // Format columns
    const followersColumn = sheet.getRange(startRow, 6, rows.length, 1);
    followersColumn.setNumberFormat('#,##0');
    
    const engagementColumn = sheet.getRange(startRow, 7, rows.length, 1);
    engagementColumn.setNumberFormat('0.00');
    
    sheet.autoResizeColumns(1, 11);
  }
}

// ==================== CORE FUNCTIONS ====================

function getFollowerRange(tierFrom, tierTo) {
  if (!tierFrom || !tierTo) {
    return { min: 0, max: 999999999 };
  }
  
  const fromTier = FOLLOWER_TIERS[tierFrom];
  const toTier = FOLLOWER_TIERS[tierTo];
  
  if (!fromTier || !toTier) {
    return { min: 0, max: 999999999 };
  }
  
  return {
    min: fromTier.min,
    max: toTier.max
  };
}

function getFollowerTier(followerCount) {
  if (followerCount >= 10000000) return 'Mega';
  if (followerCount >= 1000000) return 'Macro';
  if (followerCount >= 100000) return 'Mid';
  if (followerCount >= 10000) return 'Micro';
  if (followerCount >= 1000) return 'Nano';
  return 'Below';
}

function filterByFollowerRange(kols, minFollowers, maxFollowers) {
  if (!kols || !Array.isArray(kols)) {
    return [];
  }
  
  return kols.filter(function(kol) {
    const followers = kol.followersCount || (kol.authorMeta && kol.authorMeta.fans) || 0;
    return followers >= minFollowers && followers <= maxFollowers;
  });
}

function doGet() {
  return HtmlService.createTemplateFromFile('Index')
    .evaluate()
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .setTitle('KOL Scouting System');
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Input KOL')
    .addItem('Start KOL scouting', 'openKOLUI')
    .addSeparator()
    .addToUi();
}

function openKOLUI() {
  const html = HtmlService.createTemplateFromFile('Index')
    .evaluate()
    .setWidth(800)
    .setHeight(600)
    .setTitle('KOL Scouting');
  
  SpreadsheetApp.getUi().showModalDialog(html, 'KOL Scouting');
}

function extractKeywords(briefText, maxItems = 15) {
  try {
    resetStatus();
    updateStatus('🤖 Analyzing brief and extracting keywords...');
    const prompt2Sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Prompt');
    let prompt = prompt2Sheet.getRange('D2').getValue();

    const payload = {
      model: CONFIG.OPENAI_MODEL,
      messages: [
        { role: 'system', content: prompt },
        { role: 'user', content: briefText }
      ],
      temperature: 0
    };

    const response = UrlFetchApp.fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + CONFIG.OPENAI_API_KEY,
        'Content-Type': 'application/json'
      },
      payload: JSON.stringify(payload)
    });

    const data = JSON.parse(response.getContentText());
    
    if (data.error) {
      throw new Error('OpenAI API Error: ' + data.error.message);
    }

    const content = data.choices?.[0]?.message?.content || '';
    console.log('extractKeywords raw content:', content);
    const parsed = extractJSON(content);
    let keywords = Array.isArray(parsed.keywords) ? parsed.keywords : [];

    if (keywords.length === 0 && content) {
      keywords = content
        .split(/[,\n]/)
        .map(s => s.replace(/^[-•\d\.\s]+/, '').trim())
        .filter(Boolean);
    }

    const cleaned = [];
    const seen = new Set();
    
    for (let keyword of keywords) {
      const clean = String(keyword).trim().replace(/['"]/g, '');
      if (clean && !seen.has(clean)) {
        seen.add(clean);
        cleaned.push(clean);
      }
    }
    
    return cleaned.slice(0, maxItems);
    
  } catch (error) {
    console.error('Error in extractKeywords:', error);
    throw error;
  }
}

function searchKOLsWithKeywords(keywords, options) {
  try {
    console.log('Starting KOL search with keywords:', JSON.stringify(keywords));
    console.log('Options:', JSON.stringify(options));
    
    if (!keywords || keywords.length === 0) {
      throw new Error('No keywords found for searching');
    }
    
    updateStatus('✓ Using keywords: ' + keywords.join(', '));
    
    const followerRange = getFollowerRange(options.followerTierFrom, options.followerTierTo);
    updateStatus('✓ Follower range: ' + followerRange.min.toLocaleString() + ' - ' + followerRange.max.toLocaleString());
    
    updateStatus('🔤 Processing keywords...');
    const keywordsTh = translateToThai(keywords, false);
    if (!keywordsTh || keywordsTh.length === 0) {
      throw new Error('Unable to process keywords');
    }
    
    updateStatus('✓ Validated keywords: ' + keywordsTh.join(', '));
    
    let tiktokCreators = [];
    let apifyItems = [];

    if (options.useTikTokAPI) {
      updateStatus('📱 Searching TikTok Business API for KOLs...');
      tiktokCreators = getAllTikTokKOLs(keywordsTh, followerRange.min, followerRange.max);
    } else {
      updateStatus('ℹ️ Skipping TikTok Business API search (per settings)');
    }

    if (options.useApify) {
      updateStatus('🌐 Searching Apify TikTok Scraper...');
      apifyItems = getApifyKOLs(keywordsTh, options.maxProfilesPerQuery || 20, followerRange.max); 
      apifyItems = filterByFollowerRange(apifyItems, followerRange.min, followerRange.max);
      updateStatus('✓ Filtered Apify results by follower range');
    }
    
    if (tiktokCreators.length === 0 && apifyItems.length === 0) {
      throw new Error('No KOLs found for the selected keywords and follower range');
    }
    
    updateStatus('🔄 Processing and merging result sets...');
    const normalizedApify = normalizeApifyData(apifyItems);
    const normalizedTiktok = normalizeTikTokData(tiktokCreators);
    const allKOLs = mergeAndDedupeKOLs(normalizedApify, normalizedTiktok);
    
    if (!allKOLs || allKOLs.length === 0) {
      throw new Error('No KOLs found from the chosen data sources');
    }
    
    updateStatus('📊 Writing data to the sheet...');
    appendToSheet(allKOLs);
    
    const tiktokCount = allKOLs.filter(k => k.tiktokRegistered === 'Yes').length;
    const apifyCount = allKOLs.filter(k => k.tiktokRegistered === 'No').length;
    
    const session = getCurrentSession();
    const activeSheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    updateStatus(`✅ KOL search complete! Found ${allKOLs.length} creators (Sheet: ${activeSheet.getName()})`);
    
    return {
      success: true,
      message: 'Found ' + allKOLs.length + ' KOLs (Sheet: ' + activeSheet.getName() + ')',
      totalKOLs: allKOLs.length,
      tiktokKOLs: tiktokCount,
      apifyKOLs: apifyCount,
      keywords: keywordsTh,
      followerRange: followerRange,
      sessionInfo: session
    };
    
  } catch (error) {
    console.error('Error in searchKOLsWithKeywords:', error);
    updateStatus('❌ Error: ' + error.message);
    return {
      success: false,
      message: error.message
    };
  }
}

function startKOLScouting(briefText, options) {
  try {
    console.log('Starting KOL scouting with options:', JSON.stringify(options));
    
    updateStatus('🤖 Analyzing brief and extracting keywords...');
    const keywords = extractKeywords(briefText);
    if (!keywords || keywords.length === 0) {
      throw new Error('No keywords detected from the brief');
    }
    updateStatus('✓ Identified ' + keywords.length + ' keyword(s): ' + keywords.join(', '));
    
    return searchKOLsWithKeywords(keywords, options);
    
  } catch (error) {
    console.error('Error in startKOLScouting:', error);
    updateStatus('❌ Error: ' + error.message);
    return {
      success: false,
      message: error.message
    };
  }
}

function translateToThai(keywords, shouldTranslate = false) {
  if (!keywords || keywords.length === 0) return keywords;
  
  const clipped = keywords.slice(0, 50).map(k => String(k).substring(0, 60));
  
  let systemMsg, userMsg;
  
  if (shouldTranslate) {
    systemMsg = `You are a Thai digital marketing specialist. Translate each keyword into natural Thai search terms suitable for finding KOLs on TikTok Thailand.

Translation rules:
1. Use the Thai wording people naturally search for on social media.
2. If an English term is already widely used (e.g., Beauty, Fashion), keep it in English.
3. Ensure the phrasing fits influencer marketing context.
4. Respond in JSON only using the format: {"keywords": ["keyword1", "keyword2", ...]}`;

    userMsg = 'Translate the following keywords into Thai search terms:\n' + clipped.join(', ');
  } else {
    systemMsg = `You act as a spell checker for keywords.

Validation rules:
1. For Thai words, verify spelling and grammar.
2. For English words, ensure correct spelling and keep the original language.
3. Only fix mistakes—do not translate.
4. Respond in JSON only using the format: {"keywords": ["keyword1", "keyword2", ...]}`;

    userMsg = 'Verify the spelling of the following keywords:\n' + clipped.join(', ');
  }

  const payload = {
    model: CONFIG.OPENAI_MODEL,
    messages: [
      { role: 'system', content: systemMsg },
      { role: 'user', content: userMsg }
    ],
    temperature: 0
  };

  const response = UrlFetchApp.fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + CONFIG.OPENAI_API_KEY,
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload)
  });

  const data = JSON.parse(response.getContentText());
  const content = data.choices[0].message.content;
  const parsed = extractJSON(content);
  const processedKeywords = parsed.keywords || clipped;
  
  return processedKeywords.map(k => String(k).trim()).filter(k => k);
}

function getAllTikTokKOLs(keywords, minFollowers, maxFollowers) {
  const allCreators = [];
  const seenHandles = new Set();
  
  for (let keyword of keywords) {
    try {
      const creators = callTikTokDiscoverAPI(keyword, minFollowers, maxFollowers);
      
      for (let creator of creators) {
        const handleName = creator.handle_name || '';
        if (!handleName || seenHandles.has(handleName)) continue;
        
        seenHandles.add(handleName);
        
        const profileData = callTikTokProfileAPI(handleName);
        
        const mergedData = {
          ...creator,
          ...profileData,
          source_keyword: keyword,
          tiktok_registered: 'Yes'
        };
        
        allCreators.push(mergedData);
      }
    } catch (error) {
      console.warn('Error fetching TikTok data for keyword: ' + keyword, error);
    }
  }
  
  return allCreators;
}

function callTikTokDiscoverAPI(keyword, minFollowers, maxFollowers) {
  try {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/tto/tcm/creator/discover/';
    const params = {
      tto_tcm_account_id: CONFIG.TTO_TCM_ACCOUNT_ID,
      country_codes: JSON.stringify(['TH']),
      keyword_search: JSON.stringify([keyword]),
      page_size: '100'
    };
    
    if (minFollowers !== undefined && minFollowers > 0) {
      params.min_followers = minFollowers.toString();
    }
    if (maxFollowers !== undefined && maxFollowers < 999999999) {
      params.max_followers = maxFollowers.toString();
    }
    
    const paramString = Object.keys(params).map(key => key + '=' + encodeURIComponent(params[key])).join('&');
    
    const response = UrlFetchApp.fetch(url + '?' + paramString, {
      method: 'GET',
      headers: {
        'Access-Token': CONFIG.TIKTOK_ACCESS_TOKEN
      }
    });
    
    const data = JSON.parse(response.getContentText());
    
    if (data.code === 0 && data.data && data.data.creators) {
      return data.data.creators;
    }
    
    return [];
  } catch (error) {
    console.warn('TikTok API error for keyword: ' + keyword, error);
    return [];
  }
}

function callTikTokProfileAPI(handleName) {
  try {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/tto/tcm/creator/public/';
    const params = {
      tto_tcm_account_id: CONFIG.TTO_TCM_ACCOUNT_ID,
      handle_name: handleName
    };
    
    const paramString = Object.keys(params).map(key => key + '=' + encodeURIComponent(params[key])).join('&');
    
    const response = UrlFetchApp.fetch(url + '?' + paramString, {
      method: 'GET',
      headers: {
        'Access-Token': CONFIG.TIKTOK_ACCESS_TOKEN
      }
    });
    
    const data = JSON.parse(response.getContentText());
    
    if (data.code === 0 && data.data) {
      return data.data;
    }
    
    return {};
  } catch (error) {
    console.warn('TikTok Profile API error for handle: ' + handleName, error);
    return {};
  }
}

function getApifyKOLs(keywords, maxProfilesPerQuery, maxFollowers) {
  try {
    const keywordTokens = keywords.map(k => keywordToToken(k)).filter(h => h);
    
    if (keywordTokens.length === 0) return [];
    
    if (maxProfilesPerQuery < 1) maxProfilesPerQuery = 1;
    if (maxProfilesPerQuery > 100) maxProfilesPerQuery = 100;
    
    if (!maxFollowers || maxFollowers > 999999999) maxFollowers = 999999999;
    
    const runInput = {
      hashtags: keywordTokens,
      resultsPerPage: 20,
      profileScrapeSections: ['videos'],
      profileSorting: 'latest',
      excludePinnedPosts: false,
      searchSection: '',
      maxProfilesPerQuery: maxProfilesPerQuery,
      maxFollowersPerProfile: maxFollowers,
      scrapeRelatedVideos: false,
      shouldDownloadVideos: false,
      shouldDownloadCovers: false,
      shouldDownloadSubtitles: false,
      shouldDownloadSlideshowImages: false,
      shouldDownloadAvatars: false,
      shouldDownloadMusicCovers: false,
      proxyCountryCode: 'TH'
    };
    
    const runResponse = UrlFetchApp.fetch('https://api.apify.com/v2/acts/' + CONFIG.APIFY_ACTOR_ID + '/runs', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + CONFIG.APIFY_API_TOKEN,
        'Content-Type': 'application/json'
      },
      payload: JSON.stringify(runInput)
    });
    
    const runData = JSON.parse(runResponse.getContentText());
    const runId = runData.data.id;
    
    let isCompleted = false;
    let attempts = 0;
    const maxAttempts = 30;
    
    while (!isCompleted && attempts < maxAttempts) {
      Utilities.sleep(10000);
      
      const statusResponse = UrlFetchApp.fetch('https://api.apify.com/v2/actor-runs/' + runId, {
        headers: {
          'Authorization': 'Bearer ' + CONFIG.APIFY_API_TOKEN
        }
      });
      
      const statusData = JSON.parse(statusResponse.getContentText());
      const status = statusData.data.status;
      
      if (status === 'SUCCEEDED' || status === 'FAILED') {
        isCompleted = true;
      }
      
      attempts++;
    }
    
    const resultsResponse = UrlFetchApp.fetch('https://api.apify.com/v2/datasets/' + runData.data.defaultDatasetId + '/items', {
      headers: {
        'Authorization': 'Bearer ' + CONFIG.APIFY_API_TOKEN
      }
    });
    
    const items = JSON.parse(resultsResponse.getContentText());
    
    const thaiItems = items.filter(item => item.textLanguage === 'th');
    
    return thaiItems.map(item => ({
      ...item,
      tiktok_registered: 'No',
      sourceKeyword: item.input || item.searchHashtag?.name || null
    }));
    
  } catch (error) {
    console.warn('Apify API error:', error);
    return [];
  }
}

function normalizeApifyData(items) {
  return items.map(item => {
    const author = item.authorMeta || {};
    const engagementRate = calculateEngagementRate(item);
    const followers = author.fans || 0;
    
    return {
      handleName: author.name || '',
      nickname: author.nickName || '',
      profileUrl: author.profileUrl || '',
      followersCount: followers,
      followerTier: getFollowerTier(followers),
      bio: author.signature || '',
      engagementRate: engagementRate / 100,
      profileImage: author.avatar || '',
      creatorPrice: 'None',
      tiktokRegistered: item.tiktok_registered || 'No',
      source: 'Apify',
      textLanguage: item.textLanguage || '',
      sourceKeyword: item.sourceKeyword || null
    };
  });
}

function normalizeTikTokData(creators) {
  return creators.map(creator => {
    const creatorPrice = creator.creator_price || 0;
    const priceDisplay = creatorPrice > 0 ? 'THB ' + creatorPrice.toLocaleString() : 'N/A';
    const followers = creator.followers_count || 0;
    
    return {
      handleName: creator.handle_name || '',
      nickname: creator.nickname || '',
      profileUrl: 'https://www.tiktok.com/@' + (creator.handle_name || ''),
      followersCount: followers,
      followerTier: getFollowerTier(followers),
      bio: creator.bio || '',
      engagementRate: creator.engagement_rate || 0,
      profileImage: creator.profile_image || '',
      creatorPrice: priceDisplay,
      tiktokRegistered: creator.tiktok_registered || 'Yes',
      source: 'TikTok API',
      sourceKeyword: creator.source_keyword || '',
      textLanguage: 'th'
    };
  });
}

function mergeAndDedupeKOLs(apifyKOLs, tiktokKOLs) {
  const mergedMap = new Map();
  
  for (let kol of apifyKOLs) {
    const handle = (kol.handleName || '').trim().toLowerCase();
    if (handle && !mergedMap.has(handle)) {
      mergedMap.set(handle, kol);
    }
  }
  
  for (let kol of tiktokKOLs) {
    const handle = (kol.handleName || '').trim().toLowerCase();
    if (handle) {
      mergedMap.set(handle, kol);
    }
  }
  
  const merged = Array.from(mergedMap.values());
  merged.sort((a, b) => (b.followersCount || 0) - (a.followersCount || 0));
  
  return merged;
}

function calculateEngagementRate(item) {
  const author = item.authorMeta || {};
  const shareCount = item.shareCount || 0;
  const playCount = item.playCount || 0;
  const commentCount = item.commentCount || 0;
  const diggCount = item.diggCount || 0;
  const followers = author.fans || 1;
  
  const totalEngagement = shareCount + commentCount + diggCount;
  const engagementRate = playCount > 0 ? (totalEngagement / playCount) * 100 : 0;
  
  return Math.round(engagementRate * 100) / 100;
}

function updateStatus(message) {
  try {
    const activeSheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const sheetName = activeSheet.getName();
    const fullMessage = `[${sheetName}] ${message}`;
    PropertiesService.getScriptProperties().setProperty('CURRENT_STATUS', fullMessage);
    console.log('Status:', fullMessage);
  } catch (error) {
    console.error('Error updating status:', error);
  }
}

function getProgress() {
  try {
    const status = PropertiesService.getScriptProperties().getProperty('CURRENT_STATUS');
    return status || 'Ready';
  } catch (error) {
    console.error('Error getting status:', error);
    return 'Unable to fetch status';
  }
}

function resetStatus() {
  try {
    PropertiesService.getScriptProperties().setProperty('CURRENT_STATUS', 'Ready');
  } catch (error) {
    console.error('Error resetting status:', error);
  }
}

function clearCurrentSheet() {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    sheet.clear();
    return { success: true, message: 'Data cleared successfully' };
  } catch (error) {
    return { success: false, message: 'Error: ' + error.message };
  }
}

function processUploadedFile(base64Content, fileName, mimeType) {
  try {
    console.log('Processing file:', fileName, 'Type:', mimeType);
    
    const bytes = Utilities.base64Decode(base64Content);
    
    let content = '';
    
    if (fileName.toLowerCase().endsWith('.txt')) {
      content = Utilities.newBlob(bytes).getDataAsString('UTF-8');
      
    } else if (fileName.toLowerCase().endsWith('.pdf')) {
      return {
        success: false,
        message: 'PDF files require manual text extraction. Please copy and paste the text content instead.'
      };
      
    } else if (fileName.toLowerCase().includes('.doc')) {
      return {
        success: false,
        message: 'DOCX files require manual text extraction. Please copy and paste the text content instead.'
      };
      
    } else {
      return {
        success: false,
        message: 'Unsupported file type. Please use TXT files or copy and paste content manually.'
      };
    }
    
    content = content.trim();
    
    if (content.length === 0) {
      return {
        success: false,
        message: 'File appears to be empty or contains no readable text.'
      };
    }
    
    if (content.length > 10000) {
      content = content.substring(0, 10000) + '\n\n[Content truncated - file too long]';
    }
    
    return {
      success: true,
      content: content,
      message: 'File processed successfully'
    };
    
  } catch (error) {
    console.error('Error processing file:', error);
    return {
      success: false,
      message: 'Error processing file: ' + error.message
    };
  }
}

function processTextContent(textContent) {
  try {
    if (!textContent || textContent.trim().length === 0) {
      return {
        success: false,
        message: 'No text content provided'
      };
    }
    
    let content = textContent.trim();
    
    if (content.length > 10000) {
      content = content.substring(0, 10000) + '\n\n[Content truncated - text too long]';
    }
    
    return {
      success: true,
      content: content,
      message: 'Text content processed successfully'
    };
    
  } catch (error) {
    console.error('Error processing text content:', error);
    return {
      success: false,
      message: 'Error processing text: ' + error.message
    };
  }
}

function extractJSON(text) {
  try {
    return JSON.parse(text.trim());
  } catch (e) {
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');
    if (start !== -1 && end !== -1 && end > start) {
      try {
        return JSON.parse(text.substring(start, end + 1));
      } catch (e2) {
        return {};
      }
    }
    return {};
  }
}

function keywordToToken(s) {
  return s.trim()
    .replace(/[^0-9A-Za-z\u0E00-\u0E7F\s]/g, '')
    .replace(/\s+/g, '');
}

function removeAutoScoreTrigger() {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    let deletedCount = 0;
    
    for (let trigger of triggers) {
      if (trigger.getHandlerFunction() === 'processMultiplePromptsUnified') {
        ScriptApp.deleteTrigger(trigger);
        deletedCount++;
      }
    }
    
    Logger.log(`Removed ${deletedCount} auto-score trigger(s)`);
    return { success: true, message: `Auto-score disabled (removed ${deletedCount} trigger${deletedCount === 1 ? '' : 's'})` };
  } catch (error) {
    console.error('Error removing trigger:', error);
    return { success: false, message: error.message };
  }
}

// Export to sheet (legacy function - kept for compatibility)
function exportToSheet(kols) {
  const sheet = getCurrentSessionSheet();
  
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    const rangeToClear = sheet.getRange(2, 1, lastRow - 1, 12);
    rangeToClear.clear();
  }
  
  const rows = [];
  kols.forEach((kol, index) => {
    const engagementRate = kol.engagementRate < 1 ? kol.engagementRate * 100 : kol.engagementRate;
    
    rows.push([
      index,
      kol.profileUrl || '',
      kol.handleName || '',
      kol.nickname || '',
      (kol.bio || '').substring(0, 200),
      kol.followersCount || 0,
      Math.round(engagementRate * 100) / 100,
      kol.creatorPrice || 'None',
      kol.tiktokRegistered || 'Unknown',
      kol.source || 'Unknown',
      kol.sourceKeyword || ''
    ]);
  });
  
  if (rows.length > 0) {
    const dataRange = sheet.getRange(2, 1, rows.length, 11);
    dataRange.setValues(rows);
    
    const followersColumn = sheet.getRange(2, 6, rows.length, 1);
    followersColumn.setNumberFormat('#,##0');
    
    const engagementColumn = sheet.getRange(2, 7, rows.length, 1);
    engagementColumn.setNumberFormat('0.00');
    
    sheet.autoResizeColumns(1, 11);
  }
}
