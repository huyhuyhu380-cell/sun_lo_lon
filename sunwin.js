// ============================================================
// PHẦN 1/5: IMPORTS + CONFIG + GLOBAL STATE
// ============================================================
import fastify from "fastify";
import cors from "@fastify/cors";
import WebSocket from "ws";
import * as path from "node:path";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";

// --- CẤU HÌNH ---
const PORT = 3000;
const WS_URL = "wss://websocket.azhkthg1.net/websocket?token=";
const TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJzc2NoaWNobWVtIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzI2OTA1OTg1LCJhZmZJZCI6InN1bndpbiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoic3VuLndpbiIsInRpbWVzdGFtcCI6MTc2NTQ2OTYxNjg3MCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDAyOjgwMDo2ZjVmOmNiYzU6ODRjMTo2YzQzOjhmZGQ6NDdkYSIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMTkucG5nIiwicGxhdGZvcm1JZCI6MiwidXNlcklkIjoiOWQyMTliNGYtMjQxYS00ZmU2LTkyNDItMDQ5MWYxYzRhMDVjIiwicmVnVGltZSI6MTc2MzcyNzkwNzk0MCwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJTQ19naWF0aGluaDIxMzMifQ.XGiELjKKAgIc-0dKYjZFOlDeH2e-LC_PvrvrzPcdY1U";

// --- GLOBAL STATE ---
let rikResults = [];
let rikCurrentSession = null;
let rikWS = null;
let rikIntervalCmd = null;

// --- WEBSOCKET CLIENTS (cho endpoint /ws/sunwin/tx) ---
const wsClients = new Set();
let lstmModel = null;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// ============================================================
// PHẦN 2/5: PATTERN DATABASE + DATASET ANALYZER
// ============================================================

// --- PATTERN DATABASE ĐẦY ĐỦ ---
const PATTERN_DATABASE = {
    '1-1': ['tx', 'xt'],
    'bệt': ['tt', 'xx'],
    '2-2': ['ttxx', 'xxtt'],
    '3-3': ['tttxxx', 'xxxttt'],
    '4-4': ['ttttxxxx', 'xxxxtttt'],
    '5-5': ['tttttxxxxx', 'xxxxxttttt'],
    '1-2-1': ['txxxt', 'xtttx'],
    '2-1-2': ['ttxtt', 'xxtxx'],
    '1-2-3': ['txxttt', 'xttxxx'],
    '3-2-3': ['tttxttt', 'xxxtxxx'],
    '4-2-4': ['ttttxxtttt', 'xxxxttxxxx'],
    '3-1-3': ['tttxttt', 'xxxtxxx'],
    '1-3-1': ['txtttx', 'xtxxxt'],
    '2-3-2': ['ttxxtt', 'xxttxx'],
    '3-4-3': ['tttxxxxttt', 'xxxttttxxx'],
    '4-3-4': ['ttttxxxtttt', 'xxxxtttxxxx'],
    '1-2-1-2': ['txxxtx', 'xtttxt'],
    '2-1-2-1': ['ttxttx', 'xxtxxt'],
    '1-1-2-2': ['txttxx', 'xtxxxt'],
    '2-2-1-1': ['ttxxtx', 'xxttxx'],
    '3-2-1': ['tttxtx', 'xxxtxt'],
    '1-2-2-1': ['txxxttx', 'xtttxxt'],
    'zigzag': ['txt', 'xtx'],
    'double_zigzag': ['txtxt', 'xtxtx'],
    'triple_zigzag': ['txtxtxt', 'xtxtxtx'],
    'quad_alternate': ['txtxtxtx', 'xtxtxtxt'],
    'penta_alternate': ['txtxtxtxtx', 'xtxtxtxtxt'],
    '1-1-1-2': ['txttx', 'xtxxt'],
    '2-1-1-1': ['ttxtx', 'xxtxt'],
    '1-2-2-2': ['txxxtt', 'xtttxx'],
    '2-2-2-1': ['ttxxttx', 'xxttxx'],
    '3-3-2': ['tttxxxtt', 'xxxttxx'],
    '2-3-3': ['ttxxttt', 'xxttxxx'],
    'fibonacci_1': ['t', 'x'],
    'fibonacci_2': ['tx', 'xt'],
    'fibonacci_3': ['txt', 'xtx'],
    'fibonacci_4': ['txttx', 'xtxxt'],
    'fibonacci_5': ['txttxttx', 'xtxtxxxt'],
    'triangle': ['txx', 'xtt'],
    'square': ['ttxx', 'xxtt'],
    'pentagon': ['tttxx', 'xxxtt'],
    'hexagon': ['ttttxx', 'xxxxxt'],
    'wave_2': ['ttxx', 'xxtt'],
    'wave_3': ['tttxxx', 'xxxttt'],
    'wave_4': ['ttttxxxx', 'xxxxtttt'],
    'wave_5': ['tttttxxxxx', 'xxxxxttttt'],
    'reverse_1': ['ttx', 'xxt'],
    'reverse_2': ['ttxx', 'xxtt'],
    'reverse_3': ['tttxxx', 'xxxttt'],
    'reverse_4': ['ttttxxxx', 'xxxxtttt'],
    'interlace_1': ['txtxt', 'xtxtx'],
    'interlace_2': ['ttxxtt', 'xxttxx'],
    'interlace_3': ['tttxxttt', 'xxxxtxxx'],
    'branch_1': ['ttxtx', 'xxtxt'],
    'branch_2': ['ttxxttx', 'xxttxx'],
    'branch_3': ['tttxxtttx', 'xxxxtxxxt'],
    'spiral_1': ['txxxt', 'xtttx'],
    'spiral_2': ['ttxxxtt', 'xxtttxx'],
    'spiral_3': ['tttxxxxttt', 'xxxttttxxx'],
    'arithmetic_1': ['tx', 'xt'],
    'arithmetic_2': ['txx', 'xtt'],
    'arithmetic_3': ['txxx', 'xttt'],
    'arithmetic_4': ['txxxx', 'xtttt'],
    'geometric_1': ['tx', 'xt'],
    'geometric_2': ['txx', 'xtt'],
    'geometric_3': ['txxx', 'xttt'],
    'geometric_4': ['txxxx', 'xtttt'],
    'mixed_1': ['ttxtxx', 'xxtxtt'],
    'mixed_2': ['txxxttx', 'xtttxxt'],
    'mixed_3': ['tttxxtxx', 'xxxxttxx'],
    'mixed_4': ['txttxtxt', 'xtxtxtxt'],
    'mixed_5': ['ttxxtxtt', 'xxtxtxxt'],
    'symmetry_1': ['txt', 'xtx'],
    'symmetry_2': ['ttxxtt', 'xxttxx'],
    'symmetry_3': ['tttxxxttt', 'xxxxttxxx'],
    'symmetry_4': ['ttttxxxxtttt', 'xxxxxtttxxxx'],
    'repeat_1': ['tt', 'xx'],
    'repeat_2': ['tttt', 'xxxx'],
    'repeat_3': ['tttttt', 'xxxxxx'],
    'repeat_4': ['tttttttt', 'xxxxxxxx'],
    'alternate_1': ['txtx', 'xtxt'],
    'alternate_2': ['txtxtx', 'xtxtxt'],
    'alternate_3': ['txtxtxtx', 'xtxtxtxt'],
    'alternate_4': ['txtxtxtxtx', 'xtxtxtxtxt'],
};

// --- UTILITIES ---
function parseLines(lines) {
    try {
        const arr = lines.map(l => (typeof l === 'string' ? JSON.parse(l) : l));
        return arr.map(item => ({
            session: Number(item.session) || 0,
            dice: Array.isArray(item.dice) ? item.dice : [],
            total: Number(item.total) || 0,
            result: item.result || '',
            tx: (Number(item.total) || 0) >= 11 ? 'T' : 'X'
        })).sort((a, b) => a.session - b.session);
    } catch (e) {
        console.error("Lỗi parseLines:", e.message);
        return [];
    }
}

// --- DATASET ANALYZER (ttoan10kmaucau.txt) ---
class PatternDatasetAnalyzer {
    constructor() {
        this.patterns = [];
        this.nGramStats = {};
        this.transitionMatrix = {};
        this.patternFreq = {};
        this.maxLen = 0;
        this.minLen = 99;
        this.totalSamples = 0;
    }

    loadFromFile(filePath) {
        try {
            const content = fs.readFileSync(filePath, 'utf-8');
            const lines = content.split('\n');
            
            for (const line of lines) {
                const match = line.match(/^\d+\.\s+([TX]+)\s*-\s*([TX])/);
                if (!match) continue;
                
                const patternStr = match[1].toLowerCase();
                const nextResult = match[2].toLowerCase();
                
                this.patterns.push({ pattern: patternStr, next: nextResult });
                this.maxLen = Math.max(this.maxLen, patternStr.length);
                this.minLen = Math.min(this.minLen, patternStr.length);
                this.totalSamples++;
            }
            
            console.log(`📚 Đã load ${this.totalSamples} mẫu cầu (độ dài ${this.minLen}-${this.maxLen})`);
            this.buildNGramStats();
            this.buildTransitionMatrix();
            this.buildPatternFrequency();
            return true;
        } catch (e) {
            console.error('❌ Không load được dataset:', e.message);
            return false;
        }
    }

    buildNGramStats() {
        for (let n = 1; n <= 6; n++) this.nGramStats[n] = {};
        for (const { pattern, next } of this.patterns) {
            for (let n = 1; n <= 6; n++) {
                for (let i = 0; i <= pattern.length - n; i++) {
                    const gram = pattern.substr(i, n);
                    if (!this.nGramStats[n][gram]) {
                        this.nGramStats[n][gram] = { t: 0, x: 0, total: 0 };
                    }
                    this.nGramStats[n][gram][next]++;
                    this.nGramStats[n][gram].total++;
                }
            }
        }
    }

    buildTransitionMatrix() {
        for (const { pattern, next } of this.patterns) {
            for (let n = 1; n <= 5; n++) {
                if (pattern.length < n) continue;
                const key = pattern.slice(-n);
                const matrixKey = `${n}:${key}`;
                if (!this.transitionMatrix[matrixKey]) {
                    this.transitionMatrix[matrixKey] = { t: 0, x: 0 };
                }
                this.transitionMatrix[matrixKey][next]++;
            }
        }
    }

    buildPatternFrequency() {
        for (const { pattern } of this.patterns) {
            for (let len = 2; len <= 8; len++) {
                for (let i = 0; i <= pattern.length - len; i++) {
                    const sub = pattern.substr(i, len);
                    this.patternFreq[sub] = (this.patternFreq[sub] || 0) + 1;
                }
            }
        }
    }

    queryPattern(recentStr, maxLen = 8) {
        const results = [];
        const queryLower = recentStr.toLowerCase();
        for (const { pattern, next } of this.patterns) {
            const compareLen = Math.min(pattern.length, queryLower.length, maxLen);
            let matchLen = 0;
            for (let k = 1; k <= compareLen; k++) {
                if (pattern[pattern.length - k] === queryLower[queryLower.length - k]) {
                    matchLen = k;
                } else break;
            }
            if (matchLen >= 4) {
                results.push({ matchLen, pattern, next, weight: Math.pow(matchLen, 1.5) });
            }
        }
        return results;
    }

    getNGramProbability(recentStr, n) {
        const queryLower = recentStr.toLowerCase();
        if (queryLower.length < n) return null;
        const key = queryLower.slice(-n);
        const stat = this.nGramStats[n]?.[key];
        if (!stat || stat.total < 5) return null;
        return { t: stat.t / stat.total, x: stat.x / stat.total, total: stat.total };
    }

    getMarkovPrediction(recentStr, n) {
        const queryLower = recentStr.toLowerCase();
        if (queryLower.length < n) return null;
        const key = queryLower.slice(-n);
        const stat = this.transitionMatrix[`${n}:${key}`];
        if (!stat || (stat.t + stat.x) < 10) return null;
        const total = stat.t + stat.x;
        return { t: stat.t / total, x: stat.x / total, total };
    }
}

// Khởi tạo dataset
const dataset = new PatternDatasetAnalyzer();

const DATASET_PATHS = [
    path.join(__dirname, 'ttoan10kmaucau.txt'),
    path.join(__dirname, 'data', 'ttoan10kmaucau.txt'),
    './ttoan10kmaucau.txt'
];

for (const p of DATASET_PATHS) {
    if (dataset.loadFromFile(p)) break;
}
// ============================================================
// PHẦN 3/5: 14 THUẬT TOÁN AI + HELPER FUNCTIONS
// ============================================================

/**
 * 1. ULTRA PATTERN RECOGNITION
 */
function algo1_ultraPatternRecognition(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 30) return null;
    const txLower = tx.map(t => t.toLowerCase());
    const fullPattern = txLower.join('');
    let patternMatches = { t: 0, x: 0 };
    let totalWeight = 0;
    
    Object.entries(PATTERN_DATABASE).forEach(([patternName, patternList]) => {
        patternList.forEach(pattern => {
            const patternLength = pattern.length;
            if (patternLength > 8) return;
            for (let i = 0; i <= fullPattern.length - patternLength - 1; i++) {
                if (fullPattern.substr(i, patternLength) === pattern) {
                    const nextChar = fullPattern.charAt(i + patternLength);
                    if (nextChar === 't' || nextChar === 'x') {
                        const weight = (patternLength / 8) * (patternName.includes('complex') ? 1.5 : 1);
                        patternMatches[nextChar] += weight;
                        totalWeight += weight;
                    }
                }
            }
        });
    });
    
    if (totalWeight === 0) return null;
    const threshold = 0.65 + (Math.min(totalWeight, 50) / 100);
    const tProb = patternMatches.t / totalWeight;
    const xProb = patternMatches.x / totalWeight;
    if (tProb >= threshold) return 'T';
    if (xProb >= threshold) return 'X';
    return null;
}

/**
 * 2. QUANTUM ADAPTIVE AI
 */
function algo2_quantumAdaptiveAI(history) {
    if (history.length < 40) return null;
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    const quantumState = { t: 0.5, x: 0.5 };
    const recentCount = Math.min(20, history.length);
    
    for (let i = history.length - recentCount; i < history.length; i++) {
        const weight = 0.04;
        if (tx[i] === 'T') {
            quantumState.t *= (1 + weight);
            quantumState.x *= (1 - weight);
        } else {
            quantumState.x *= (1 + weight);
            quantumState.t *= (1 - weight);
        }
    }
    
    const recentAvg = totals.slice(-10).reduce((a, b) => a + b, 0) / 10;
    if (recentAvg > 11.2) { quantumState.t *= 0.85; quantumState.x *= 1.15; }
    else if (recentAvg < 9.8) { quantumState.t *= 1.15; quantumState.x *= 0.85; }
    
    const total = quantumState.t + quantumState.x;
    quantumState.t /= total;
    quantumState.x /= total;
    
    if (quantumState.t > 0.68) return 'T';
    if (quantumState.x > 0.68) return 'X';
    return null;
}

/**
 * 3. DEEP TREND ANALYSIS
 */
function algo3_deepTrendAnalysis(history) {
    if (history.length < 25) return null;
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    const periods = [5, 10, 15, 20];
    const trends = { t: 0, x: 0 };
    
    periods.forEach(period => {
        if (tx.length >= period) {
            const recent = tx.slice(-period);
            const tCount = recent.filter(c => c === 'T').length;
            const xCount = recent.filter(c => c === 'X').length;
            if (tCount > xCount) trends.t += 1;
            else if (xCount > tCount) trends.x += 1;
        }
    });
    
    const totalAvg = totals.reduce((a, b) => a + b, 0) / totals.length;
    const recentAvg = totals.slice(-8).reduce((a, b) => a + b, 0) / 8;
    if (recentAvg > totalAvg + 0.8) trends.t += 1.5;
    if (recentAvg < totalAvg - 0.8) trends.x += 1.5;
    
    if (trends.t > trends.x + 1.5) return 'T';
    if (trends.x > trends.t + 1.5) return 'X';
    return null;
}

/**
 * 4. SMART BRIDGE DETECTION
 */
function algo4_smartBridgeDetection(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 15) return null;
    const recentTx = tx.slice(-15);
    const lastResult = recentTx[recentTx.length - 1];
    
    let runLength = 1;
    for (let i = recentTx.length - 2; i >= 0; i--) {
        if (recentTx[i] === lastResult) runLength++;
        else break;
    }
    
    if (runLength >= 2 && runLength <= 4) {
        const patternStr = recentTx.slice(-8).join('').toLowerCase();
        const strongPatterns = ['tttt', 'xxxx', 'txtxtx', 'xtxtxt'];
        let inStrongPattern = false;
        strongPatterns.forEach(pattern => {
            if (patternStr.includes(pattern)) inStrongPattern = true;
        });
        if (inStrongPattern) return lastResult;
        
        const overallTrend = calculateOverallTrend(tx);
        if (overallTrend === lastResult) return lastResult;
    }
    
    if (runLength >= 5) return lastResult === 'T' ? 'X' : 'T';
    
    const lastPattern = recentTx.slice(-6).join('').toLowerCase();
    const reversalPatterns = ['tttxxx', 'xxxttt', 'ttxx', 'xxtt', 'txtxtx', 'xtxtxt'];
    if (reversalPatterns.includes(lastPattern)) {
        return lastResult === 'T' ? 'X' : 'T';
    }
    return null;
}

/**
 * 5. VOLATILITY PREDICTION
 */
function algo5_volatilityPrediction(history) {
    if (history.length < 30) return null;
    const totals = history.map(h => h.total);
    const recent10 = totals.slice(-10);
    const recent20 = totals.slice(-20);
    const vol10 = calculateVolatility(recent10);
    const vol20 = calculateVolatility(recent20);
    
    if (vol10 > vol20 * 1.5) {
        const avgRecent = recent10.reduce((a, b) => a + b, 0) / 10;
        if (avgRecent > 11.0) return 'X';
        if (avgRecent < 10.0) return 'T';
    } else if (vol10 < vol20 * 0.7) {
        const recentTx = history.slice(-10).map(h => h.tx);
        const tCount = recentTx.filter(t => t === 'T').length;
        const xCount = recentTx.filter(t => t === 'X').length;
        if (tCount > xCount + 2) return 'T';
        if (xCount > tCount + 2) return 'X';
    }
    return null;
}

/**
 * 6. PATTERN FUSION AI
 */
function algo6_patternFusionAI(history) {
    const tx = history.map(h => h.tx);
    if (tx.length < 35) return null;
    const txLower = tx.map(t => t.toLowerCase());
    const patterns = [];
    
    const patternTypes = [
        { name: 'basic', length: 3, weight: 0.3 },
        { name: 'advanced', length: 5, weight: 0.5 },
        { name: 'complex', length: 7, weight: 0.7 }
    ];
    
    patternTypes.forEach(type => {
        if (txLower.length >= type.length + 1) {
            const lastPattern = txLower.slice(-type.length).join('');
            let matches = { t: 0, x: 0 };
            for (let i = 0; i <= txLower.length - type.length - 1; i++) {
                if (txLower.slice(i, i + type.length).join('') === lastPattern) {
                    matches[txLower[i + type.length]]++;
                }
            }
            const total = matches.t + matches.x;
            if (total >= 2) {
                const confidence = Math.max(matches.t, matches.x) / total;
                if (confidence > 0.7) {
                    patterns.push({
                        prediction: matches.t > matches.x ? 'T' : 'X',
                        confidence: confidence * type.weight,
                        weight: type.weight
                    });
                }
            }
        }
    });
    
    if (patterns.length === 0) return null;
    const combined = { t: 0, x: 0 };
    patterns.forEach(p => {
        if (p.prediction === 'T') combined.t += p.confidence;
        else combined.x += p.confidence;
    });
    if (combined.t > combined.x * 1.3) return 'T';
    if (combined.x > combined.t * 1.3) return 'X';
    return null;
}

/**
 * 7. REAL-TIME ADAPTIVE AI
 */
function algo7_realtimeAdaptiveAI(history) {
    if (history.length < 20) return null;
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    const indicators = {
        rsi: calculateRSI(tx.slice(-14)),
        macd: calculateMACD(totals),
        bias: calculateBias(tx.slice(-20)),
        momentum: calculateMomentum(totals.slice(-10))
    };
    let tScore = 0, xScore = 0;
    if (indicators.rsi > 70) xScore += 1.5;
    else if (indicators.rsi < 30) tScore += 1.5;
    if (indicators.macd > 0.5) tScore += 1;
    else if (indicators.macd < -0.5) xScore += 1;
    if (indicators.bias > 0.6) tScore += 1.2;
    else if (indicators.bias < 0.4) xScore += 1.2;
    if (indicators.momentum > 0.3) tScore += 0.8;
    else if (indicators.momentum < -0.3) xScore += 0.8;
    if (tScore > xScore + 1.5) return 'T';
    if (xScore > tScore + 1.5) return 'X';
    return null;
}

/**
 * 8. N-GRAM MARKOV AI
 */
function algo8_nGramMarkovAI(history) {
    if (history.length < 10 || dataset.totalSamples === 0) return null;
    const tx = history.map(h => h.tx.toLowerCase());
    const recent = tx.join('');
    let bestPrediction = null;
    let bestScore = 0;
    
    for (let n = 6; n >= 2; n--) {
        const prob = dataset.getNGramProbability(recent, n);
        if (!prob || prob.total < 10) continue;
        const confidence = Math.max(prob.t, prob.x);
        const score = confidence * Math.log10(prob.total + 1) * n;
        if (score > bestScore && confidence > 0.6) {
            bestScore = score;
            bestPrediction = prob.t > prob.x ? 'T' : 'X';
        }
    }
    
    if (!bestPrediction) {
        for (let n = 4; n >= 1; n--) {
            const markov = dataset.getMarkovPrediction(recent, n);
            if (!markov || markov.total < 20) continue;
            const conf = Math.max(markov.t, markov.x);
            if (conf > 0.62) {
                bestPrediction = markov.t > markov.x ? 'T' : 'X';
                break;
            }
        }
    }
    return bestPrediction;
}

/**
 * 9. FREQUENCY PATTERN MINER
 */
function algo9_frequencyPatternMiner(history) {
    if (history.length < 15 || dataset.totalSamples === 0) return null;
    const tx = history.map(h => h.tx.toLowerCase());
    const recent = tx.slice(-10).join('');
    const matches = dataset.queryPattern(recent, 10);
    if (matches.length < 3) return null;
    
    matches.sort((a, b) => b.weight - a.weight);
    const top = matches.slice(0, 50);
    let tVotes = 0, xVotes = 0, totalWeight = 0;
    for (const m of top) {
        if (m.next === 't') tVotes += m.weight;
        else xVotes += m.weight;
        totalWeight += m.weight;
    }
    if (totalWeight === 0) return null;
    const tProb = tVotes / totalWeight;
    const xProb = xVotes / totalWeight;
    if (tProb > 0.7) return 'T';
    if (xProb > 0.7) return 'X';
    return null;
}

/**
 * 10. ADAPTIVE WINDOW MATCHER
 */
function algo10_adaptiveWindowMatcher(history) {
    if (history.length < 20 || dataset.totalSamples === 0) return null;
    const tx = history.map(h => h.tx.toLowerCase());
    const windows = [12, 10, 8, 6];
    
    for (const winSize of windows) {
        if (tx.length < winSize) continue;
        const window = tx.slice(-winSize).join('');
        const matches = [];
        for (const { pattern, next } of dataset.patterns) {
            if (pattern.length < winSize) continue;
            if (pattern.slice(-winSize) === window) matches.push(next);
        }
        if (matches.length >= 5) {
            const tCount = matches.filter(m => m === 't').length;
            const xCount = matches.length - tCount;
            const conf = Math.max(tCount, xCount) / matches.length;
            const threshold = winSize >= 10 ? 0.7 : 0.65;
            if (conf >= threshold && matches.length >= 5) {
                return tCount > xCount ? 'T' : 'X';
            }
        }
    }
    return null;
}

/**
 * 11. STATISTICAL BIAS DETECTOR
 */
function algo11_statisticalBiasDetector(history) {
    if (history.length < 30) return null;
    const tx = history.map(h => h.tx);
    const frames = [
        { data: tx.slice(-10), weight: 1.5 },
        { data: tx.slice(-20), weight: 1.0 },
        { data: tx.slice(-30), weight: 0.7 }
    ];
    let biasScore = 0, totalWeight = 0;
    for (const frame of frames) {
        const tCount = frame.data.filter(t => t === 'T').length;
        const xCount = frame.data.length - tCount;
        const bias = (tCount - xCount) / frame.data.length;
        biasScore += bias * frame.weight;
        totalWeight += frame.weight;
    }
    biasScore /= totalWeight;
    
    const recentBias = (tx.slice(-5).filter(t => t === 'T').length - 
                        tx.slice(-5).filter(t => t === 'X').length) / 5;
    if (Math.abs(recentBias - biasScore) > 0.6) return recentBias > 0 ? 'X' : 'T';
    if (biasScore > 0.35) return 'T';
    if (biasScore < -0.35) return 'X';
    return null;
}

/**
 * 12. ENSEMBLE META-LEARNER
 */
function algo12_ensembleMetaLearner(history) {
    if (history.length < 25 || dataset.totalSamples === 0) return null;
    const subPredictions = [];
    const subAlgos = [
        algo1_ultraPatternRecognition,
        algo3_deepTrendAnalysis,
        algo4_smartBridgeDetection,
        algo8_nGramMarkovAI,
        algo9_frequencyPatternMiner,
        algo10_adaptiveWindowMatcher,
        algo11_statisticalBiasDetector
    ];
    for (const fn of subAlgos) {
        try {
            const p = fn(history);
            if (p === 'T' || p === 'X') subPredictions.push(p);
        } catch (e) {}
    }
    if (subPredictions.length < 3) return null;
    const tCount = subPredictions.filter(p => p === 'T').length;
    const xCount = subPredictions.length - tCount;
    const consensus = Math.max(tCount, xCount) / subPredictions.length;
    if (consensus < 0.7) return null;
    return tCount > xCount ? 'T' : 'X';
}

/**
 * 13. LSTM SEQUENCE AI
 */
function algo13_lstmSequenceAI(history) {
    if (!lstmModel || history.length < 10) return null;
    const tx = history.map(h => h.tx);
    const result = lstmModel.predict(tx);
    if (!result || result.confidence < 0.65) return null;
    return result.prediction;
}

/**
 * 14. LSTM + DATASET HYBRID
 */
function algo14_lstmDatasetHybrid(history) {
    if (!lstmModel || history.length < 15 || dataset.totalSamples === 0) return null;
    const tx = history.map(h => h.tx);
    const lstmResult = lstmModel.predict(tx);
    if (!lstmResult) return null;
    
    const recent = tx.slice(-10).join('').toLowerCase();
    const matches = dataset.queryPattern(recent, 10);
    if (matches.length < 5) {
        return lstmResult.confidence > 0.7 ? lstmResult.prediction : null;
    }
    
    let tVotes = 0, xVotes = 0;
    for (const m of matches) {
        if (m.next === 't') tVotes += m.weight;
        else xVotes += m.weight;
    }
    const datasetPred = tVotes > xVotes ? 'T' : 'X';
    const datasetConf = Math.max(tVotes, xVotes) / (tVotes + xVotes);
    
    if (lstmResult.prediction === datasetPred) {
        const combinedConf = (lstmResult.confidence + datasetConf) / 2;
        if (combinedConf > 0.7) return datasetPred;
    }
    if (lstmResult.confidence > datasetConf + 0.15) return lstmResult.prediction;
    if (datasetConf > lstmResult.confidence + 0.15) return datasetPred;
    return null;
}

// --- HELPER FUNCTIONS ---
function calculateVolatility(numbers) {
    const mean = numbers.reduce((a, b) => a + b, 0) / numbers.length;
    const variance = numbers.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / numbers.length;
    return Math.sqrt(variance);
}

function calculateOverallTrend(txArray) {
    if (txArray.length < 10) return null;
    const tCount = txArray.filter(t => t === 'T').length;
    const xCount = txArray.filter(t => t === 'X').length;
    if (tCount > xCount * 1.3) return 'T';
    if (xCount > tCount * 1.3) return 'X';
    return null;
}

function calculateRSI(txArray) {
    if (txArray.length < 14) return 50;
    let gains = 0, losses = 0;
    for (let i = 1; i < txArray.length; i++) {
        if (txArray[i] === 'T' && txArray[i-1] === 'X') gains++;
        else if (txArray[i] === 'X' && txArray[i-1] === 'T') losses++;
    }
    if (losses === 0) return 100;
    const rs = gains / losses;
    return 100 - (100 / (1 + rs));
}

function calculateMACD(totals) {
    if (totals.length < 26) return 0;
    const ema12 = calculateEMA(totals.slice(-12), 12);
    const ema26 = calculateEMA(totals.slice(-26), 26);
    return ema12 - ema26;
}

function calculateEMA(numbers, period) {
    const multiplier = 2 / (period + 1);
    let ema = numbers[0];
    for (let i = 1; i < numbers.length; i++) {
        ema = numbers[i] * multiplier + ema * (1 - multiplier);
    }
    return ema;
}

function calculateBias(txArray) {
    const tCount = txArray.filter(t => t === 'T').length;
    return tCount / txArray.length;
}

function calculateMomentum(numbers) {
    if (numbers.length < 2) return 0;
    return numbers[numbers.length - 1] - numbers[0];
}

// --- DANH SÁCH THUẬT TOÁN ---
const ALGORITHMS = [
    { id: 'ultra_pattern',  fn: algo1_ultraPatternRecognition,  name: 'Ultra Pattern AI' },
    { id: 'quantum_ai',     fn: algo2_quantumAdaptiveAI,        name: 'Quantum Adaptive AI' },
    { id: 'deep_trend',     fn: algo3_deepTrendAnalysis,        name: 'Deep Trend AI' },
    { id: 'smart_bridge',   fn: algo4_smartBridgeDetection,     name: 'Smart Bridge AI' },
    { id: 'volatility',     fn: algo5_volatilityPrediction,     name: 'Volatility AI' },
    { id: 'pattern_fusion', fn: algo6_patternFusionAI,          name: 'Pattern Fusion AI' },
    { id: 'realtime_ai',    fn: algo7_realtimeAdaptiveAI,       name: 'Real-time Adaptive AI' },
    { id: 'ngram_markov',   fn: algo8_nGramMarkovAI,            name: 'N-Gram Markov AI' },
    { id: 'freq_miner',     fn: algo9_frequencyPatternMiner,    name: 'Frequency Pattern Miner' },
    { id: 'window_matcher', fn: algo10_adaptiveWindowMatcher,   name: 'Adaptive Window Matcher' },
    { id: 'stat_bias',      fn: algo11_statisticalBiasDetector, name: 'Statistical Bias Detector' },
    { id: 'ensemble_meta',  fn: algo12_ensembleMetaLearner,     name: 'Ensemble Meta-Learner' },
    { id: 'lstm_seq',       fn: algo13_lstmSequenceAI,          name: 'LSTM Sequence AI' },
    { id: 'lstm_hybrid',    fn: algo14_lstmDatasetHybrid,       name: 'LSTM-Dataset Hybrid' },
];
// ============================================================
// PHẦN 4/5: LSTM MODEL + AI CORE CLASS
// ============================================================

class LSTMLikeModel {
    constructor(hiddenSize = 16, learningRate = 0.01) {
        this.hiddenSize = hiddenSize;
        this.inputSize = 2;
        this.outputSize = 2;
        this.learningRate = learningRate;

        this.Wf = this.initMatrix(hiddenSize, hiddenSize + this.inputSize);
        this.Wi = this.initMatrix(hiddenSize, hiddenSize + this.inputSize);
        this.Wo = this.initMatrix(hiddenSize, hiddenSize + this.inputSize);
        this.Wc = this.initMatrix(hiddenSize, hiddenSize + this.inputSize);

        this.bf = this.initVector(hiddenSize, 1.0);
        this.bi = this.initVector(hiddenSize, 0.0);
        this.bo = this.initVector(hiddenSize, 0.0);
        this.bc = this.initVector(hiddenSize, 0.0);

        this.Wy = this.initMatrix(this.outputSize, hiddenSize);
        this.by = this.initVector(this.outputSize, 0.0);

        this.h = new Array(hiddenSize).fill(0);
        this.c = new Array(hiddenSize).fill(0);

        this.trainSteps = 0;
        this.trainLoss = 0;
        this.lastAccuracy = 0;
    }

    initMatrix(rows, cols) {
        const m = [];
        const scale = Math.sqrt(2.0 / cols);
        for (let i = 0; i < rows; i++) {
            const row = [];
            for (let j = 0; j < cols; j++) row.push((Math.random() * 2 - 1) * scale);
            m.push(row);
        }
        return m;
    }

    initVector(size, fill = 0) { return new Array(size).fill(fill); }

    sigmoid(x) {
        if (x > 20) return 1;
        if (x < -20) return 0;
        return 1 / (1 + Math.exp(-x));
    }

    tanh(x) { return Math.tanh(x); }

    softmax(arr) {
        const max = Math.max(...arr);
        const exps = arr.map(v => Math.exp(v - max));
        const sum = exps.reduce((a, b) => a + b, 0);
        return exps.map(v => v / sum);
    }

    forward(input, h, c) {
        const combined = [...h, ...input];
        const f = [], i_gate = [], c_cand = [], o_gate = [];
        
        for (let i = 0; i < this.hiddenSize; i++) {
            let sumF = this.bf[i], sumI = this.bi[i], sumC = this.bc[i], sumO = this.bo[i];
            for (let j = 0; j < combined.length; j++) {
                sumF += this.Wf[i][j] * combined[j];
                sumI += this.Wi[i][j] * combined[j];
                sumC += this.Wc[i][j] * combined[j];
                sumO += this.Wo[i][j] * combined[j];
            }
            f.push(this.sigmoid(sumF));
            i_gate.push(this.sigmoid(sumI));
            c_cand.push(this.tanh(sumC));
            o_gate.push(this.sigmoid(sumO));
        }

        const c_new = [], h_new = [];
        for (let i = 0; i < this.hiddenSize; i++) {
            c_new.push(f[i] * c[i] + i_gate[i] * c_cand[i]);
            h_new.push(o_gate[i] * this.tanh(c_new[i]));
        }

        const logits = [];
        for (let k = 0; k < this.outputSize; k++) {
            let sum = this.by[k];
            for (let i = 0; i < this.hiddenSize; i++) sum += this.Wy[k][i] * h_new[i];
            logits.push(sum);
        }
        const probs = this.softmax(logits);
        return {
            probs,
            cache: { input, h, c, combined, f, i_gate, c_cand, c_new, o_gate, h_new, logits }
        };
    }

    forwardSequence(sequence) {
        let h = new Array(this.hiddenSize).fill(0);
        let c = new Array(this.hiddenSize).fill(0);
        let lastOutput = null;
        for (const input of sequence) {
            const out = this.forward(input, h, c);
            h = out.cache.h_new;
            c = out.cache.c_new;
            lastOutput = out;
        }
        return {
            probs: lastOutput ? lastOutput.probs : [0.5, 0.5],
            finalH: h,
            finalC: c
        };
    }

    trainStep(sequence, targetIdx) {
        let h = new Array(this.hiddenSize).fill(0);
        let c = new Array(this.hiddenSize).fill(0);
        const caches = [];
        for (const input of sequence) {
            const out = this.forward(input, h, c);
            caches.push(out.cache);
            h = out.cache.h_new;
            c = out.cache.c_new;
        }
        const lastCache = caches[caches.length - 1];
        const probs = this.softmax(lastCache.logits);
        const dLogits = probs.slice();
        dLogits[targetIdx] -= 1;
        this.trainLoss = -Math.log(Math.max(probs[targetIdx], 1e-10));

        const dWy = [];
        for (let k = 0; k < this.outputSize; k++) {
            const row = [];
            for (let i = 0; i < this.hiddenSize; i++) row.push(dLogits[k] * lastCache.h_new[i]);
            dWy.push(row);
        }
        const dh_next = new Array(this.hiddenSize).fill(0);
        for (let i = 0; i < this.hiddenSize; i++) {
            let sum = 0;
            for (let k = 0; k < this.outputSize; k++) sum += this.Wy[k][i] * dLogits[k];
            dh_next[i] = sum;
        }
        for (let k = 0; k < this.outputSize; k++) {
            this.by[k] -= this.learningRate * dLogits[k];
            for (let i = 0; i < this.hiddenSize; i++) {
                this.Wy[k][i] -= this.learningRate * dWy[k][i];
            }
        }

        const bpttSteps = Math.min(2, caches.length);
        let dc_next = new Array(this.hiddenSize).fill(0);
        let dh_carry = dh_next;

        for (let t = caches.length - 1; t >= caches.length - bpttSteps; t--) {
            const cache = caches[t];
            const prevC = t > 0 ? caches[t - 1].c_new : new Array(this.hiddenSize).fill(0);

            const dC = [], dO = [], dI = [], dCcand = [], dF = [];
            for (let i = 0; i < this.hiddenSize; i++) {
                const tanhC = this.tanh(cache.c_new[i]);
                dC.push(dh_carry[i] * cache.o_gate[i] * (1 - tanhC * tanhC) + dc_next[i]);
                dO.push(dh_carry[i] * this.tanh(cache.c_new[i]));
                dI.push(dC[i] * cache.c_cand[i]);
                dCcand.push(dC[i] * cache.i_gate[i]);
                dF.push(dC[i] * prevC[i]);
            }

            const dO_pre = [], dI_pre = [], dF_pre = [], dC_pre = [];
            for (let i = 0; i < this.hiddenSize; i++) {
                dO_pre.push(dO[i] * cache.o_gate[i] * (1 - cache.o_gate[i]));
                dI_pre.push(dI[i] * cache.i_gate[i] * (1 - cache.i_gate[i]));
                dF_pre.push(dF[i] * cache.f[i] * (1 - cache.f[i]));
                dC_pre.push(dCcand[i] * (1 - cache.c_cand[i] * cache.c_cand[i]));
            }

            const combined = cache.combined;
            for (let i = 0; i < this.hiddenSize; i++) {
                this.bf[i] -= this.learningRate * dF_pre[i];
                this.bi[i] -= this.learningRate * dI_pre[i];
                this.bo[i] -= this.learningRate * dO_pre[i];
                this.bc[i] -= this.learningRate * dC_pre[i];
                for (let j = 0; j < combined.length; j++) {
                    const g = combined[j];
                    this.Wf[i][j] -= this.learningRate * dF_pre[i] * g;
                    this.Wi[i][j] -= this.learningRate * dI_pre[i] * g;
                    this.Wo[i][j] -= this.learningRate * dO_pre[i] * g;
                    this.Wc[i][j] -= this.learningRate * dC_pre[i] * g;
                }
            }

            const dh_prev = new Array(this.hiddenSize).fill(0);
            for (let j = 0; j < this.hiddenSize; j++) {
                let sum = 0;
                for (let i = 0; i < this.hiddenSize; i++) {
                    sum += dF_pre[i] * this.Wf[i][j] + dI_pre[i] * this.Wi[i][j] +
                           dO_pre[i] * this.Wo[i][j] + dC_pre[i] * this.Wc[i][j];
                }
                dh_prev[j] = sum;
            }
            const dc_prev = new Array(this.hiddenSize).fill(0);
            for (let i = 0; i < this.hiddenSize; i++) dc_prev[i] = dC[i] * cache.f[i];

            dh_carry = dh_prev;
            dc_next = dc_prev;
        }

        this.trainSteps++;
        return this.trainLoss;
    }

    encodeSequence(txArray) {
        return txArray.map(t => t === 'T' ? [1, 0] : [0, 1]);
    }

    trainOnDataset(patterns, epochs = 1) {
        console.log(`🧠 LSTM training: ${patterns.length} mẫu × ${epochs} epoch...`);
        const startTime = Date.now();
        let totalLoss = 0, correct = 0, count = 0;

        for (let epoch = 0; epoch < epochs; epoch++) {
            for (const { pattern, next } of patterns) {
                const txArray = pattern.toUpperCase().split('');
                if (txArray.length < 5) continue;
                const sequence = this.encodeSequence(txArray);
                const targetIdx = next === 't' ? 0 : 1;
                const loss = this.trainStep(sequence, targetIdx);
                totalLoss += loss;

                const pred = this.forwardSequence(sequence);
                const predIdx = pred.probs[0] > pred.probs[1] ? 0 : 1;
                if (predIdx === targetIdx) correct++;
                count++;

                this.h = new Array(this.hiddenSize).fill(0);
                this.c = new Array(this.hiddenSize).fill(0);
            }
            const avgLoss = totalLoss / count;
            const accuracy = correct / count;
            this.lastAccuracy = accuracy;
            console.log(`   Epoch ${epoch + 1}/${epochs}: loss=${avgLoss.toFixed(4)} acc=${(accuracy * 100).toFixed(1)}%`);
            totalLoss = 0; correct = 0; count = 0;
        }
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`✅ LSTM trained in ${elapsed}s | Final acc: ${(this.lastAccuracy * 100).toFixed(1)}%`);
    }

    predict(historyTx) {
        if (historyTx.length < 5) return null;
        const recent = historyTx.slice(-20);
        const sequence = this.encodeSequence(recent);
        const result = this.forwardSequence(sequence);
        return {
            probs: result.probs,
            prediction: result.probs[0] > result.probs[1] ? 'T' : 'X',
            confidence: Math.max(result.probs[0], result.probs[1])
        };
    }

    resetState() {
        this.h = new Array(this.hiddenSize).fill(0);
        this.c = new Array(this.hiddenSize).fill(0);
    }
}

// --- KHỞI TẠO LSTM SAU KHI LOAD DATASET ---
if (dataset.totalSamples > 0) {
    lstmModel = new LSTMLikeModel(16, 0.008);
    const trainingSubset = dataset.patterns.slice(0, 3000);
    setTimeout(() => {
        try {
            lstmModel.trainOnDataset(trainingSubset, 2);
        } catch (e) {
            console.error('❌ LSTM training lỗi:', e.message);
        }
    }, 100);
} else {
    console.log('⚠️ Không có dataset → LSTM model bị vô hiệu hóa');
}

// --- ADVANCED AI CORE ---
class AdvancedDeepLearningAI {
    constructor() {
        this.history = [];
        this.algorithmWeights = {};
        this.algorithmPerformance = {};
        this.recentPredictions = {};
        this.learningRate = 0.1;

        ALGORITHMS.forEach(algo => {
            this.algorithmWeights[algo.id] = 1.0;
            this.algorithmPerformance[algo.id] = {
                correct: 0, total: 0, recent: [], streak: 0, maxStreak: 0, name: algo.name
            };
            this.recentPredictions[algo.id] = null;
        });
    }

    updateAlgorithmPerformance(actualTx) {
        ALGORITHMS.forEach(algo => {
            const perf = this.algorithmPerformance[algo.id];
            const lastPred = this.recentPredictions[algo.id];
            if (lastPred) {
                const correct = lastPred === actualTx;
                perf.correct += correct ? 1 : 0;
                perf.total += 1;
                if (correct) {
                    perf.streak++;
                    perf.maxStreak = Math.max(perf.maxStreak, perf.streak);
                } else perf.streak = 0;
                perf.recent.push(correct ? 1 : 0);
                if (perf.recent.length > 10) perf.recent.shift();
                if (perf.total >= 15) {
                    const accuracy = perf.correct / perf.total;
                    const recentAccuracy = perf.recent.reduce((a, b) => a + b) / perf.recent.length;
                    const streakBonus = perf.streak * 0.03;
                    let newWeight = (accuracy * 0.6 + recentAccuracy * 0.3 + streakBonus * 0.1);
                    newWeight = Math.max(0.1, Math.min(2.0, newWeight * 1.8));
                    this.algorithmWeights[algo.id] = this.algorithmWeights[algo.id] * 0.8 + newWeight * 0.2;
                }
            }
        });
        ALGORITHMS.forEach(algo => { this.recentPredictions[algo.id] = null; });
    }

    calculateTrueConfidence(predictions) {
        if (predictions.length === 0) return 0.5;
        const votes = { T: 0, X: 0 };
        let totalWeight = 0;
        predictions.forEach(pred => {
            const weight = this.algorithmWeights[pred.algorithm] || 1.0;
            votes[pred.prediction] += weight;
            totalWeight += weight;
        });
        if (totalWeight === 0) return 0.5;
        const tVotes = votes['T'] || 0;
        const xVotes = votes['X'] || 0;
        const winningPrediction = tVotes > xVotes ? 'T' : (xVotes > tVotes ? 'X' : null);
        if (!winningPrediction) return 0.5;
        const winningVotes = Math.max(tVotes, xVotes);
        let confidence = winningVotes / totalWeight;
        const consensus = predictions.filter(p => p.prediction === winningPrediction).length / predictions.length;
        confidence = (confidence * 0.7) + (consensus * 0.3);
        return Math.max(0.5, Math.min(0.98, confidence));
    }

    predict() {
        const minHistory = dataset.totalSamples > 0 ? 10 : 15;
        if (this.history.length < minHistory) {
            return { prediction: 'tài', confidence: 0.5, rawPrediction: 'T', algorithms: 0 };
        }
        const predictions = [];
        this.recentPredictions = {};
        ALGORITHMS.forEach(algo => {
            try {
                const pred = algo.fn(this.history);
                if (pred === 'T' || pred === 'X') {
                    const weight = this.algorithmWeights[algo.id] || 1.0;
                    predictions.push({ algorithm: algo.id, prediction: pred, weight });
                    this.recentPredictions[algo.id] = pred;
                }
            } catch (e) {
                console.error(`Lỗi thuật toán ${algo.id}:`, e.message);
            }
        });
        if (predictions.length === 0) {
            return { prediction: 'tài', confidence: 0.5, rawPrediction: 'T', algorithms: 0 };
        }
        const votes = { T: 0, X: 0 };
        predictions.forEach(p => { votes[p.prediction] += p.weight; });
        const tVotes = votes['T'] || 0;
        const xVotes = votes['X'] || 0;
        let finalPrediction = 'T';
        if (xVotes > tVotes) finalPrediction = 'X';
        else if (xVotes === tVotes) finalPrediction = this.history[this.history.length - 1].tx;
        const confidence = this.calculateTrueConfidence(predictions);
        return {
            prediction: finalPrediction === 'T' ? 'tài' : 'xỉu',
            confidence, rawPrediction: finalPrediction, algorithms: predictions.length
        };
    }

    addResult(record) {
        const parsed = {
            session: Number(record.session) || 0,
            dice: Array.isArray(record.dice) ? record.dice : [],
            total: Number(record.total) || 0,
            result: record.result || '',
            tx: (Number(record.total) || 0) >= 11 ? 'T' : 'X'
        };
        if (this.history.length >= 15) this.updateAlgorithmPerformance(parsed.tx);
        this.history.push(parsed);
        if (this.history.length > 500) this.history = this.history.slice(-400);
        return parsed;
    }

    loadHistory(historyData) {
        this.history = parseLines(historyData);
        if (this.history.length >= 30) {
            console.log(`🤖 Đang huấn luyện AI trên ${this.history.length} mẫu thực...`);
            
            if (dataset.totalSamples > 0) {
                console.log(`📚 Pre-training với ${dataset.totalSamples} mẫu từ ttoan10kmaucau.txt...`);
                for (let k = 0; k < Math.min(dataset.patterns.length, 2000); k++) {
                    const sample = dataset.patterns[k];
                    const sampleHistory = [];
                    for (let i = 0; i < sample.pattern.length; i++) {
                        sampleHistory.push({
                            session: i, tx: sample.pattern[i].toUpperCase(),
                            total: sample.pattern[i] === 't' ? 12 : 9,
                            dice: [1, 1, 1],
                            result: sample.pattern[i] === 't' ? 'Tài' : 'Xỉu'
                        });
                    }
                    if (sampleHistory.length >= 15) {
                        ALGORITHMS.forEach(algo => {
                            try {
                                const pred = algo.fn(sampleHistory);
                                if (pred) {
                                    const perf = this.algorithmPerformance[algo.id];
                                    const correct = pred === sample.next.toUpperCase();
                                    perf.correct += correct ? 1 : 0;
                                    perf.total += 1;
                                }
                            } catch (e) {}
                        });
                    }
                }
                ALGORITHMS.forEach(algo => {
                    const perf = this.algorithmPerformance[algo.id];
                    if (perf.total >= 20) {
                        const accuracy = perf.correct / perf.total;
                        this.algorithmWeights[algo.id] = Math.max(0.2, accuracy * 2);
                    }
                });
                console.log(`✅ Pre-training hoàn tất!`);
            }
            
            for (let i = 20; i < this.history.length - 1; i++) {
                const pastHistory = this.history.slice(0, i + 1);
                const actualTx = this.history[i + 1]?.tx;
                if (!actualTx) continue;
                ALGORITHMS.forEach(algo => {
                    try {
                        const pred = algo.fn(pastHistory);
                        if (pred) {
                            const perf = this.algorithmPerformance[algo.id];
                            const correct = pred === actualTx;
                            perf.recent.push(correct ? 1 : 0);
                            if (perf.recent.length > 10) perf.recent.shift();
                            perf.correct += correct ? 1 : 0;
                            perf.total++;
                            if (perf.total >= 15) {
                                const accuracy = perf.correct / perf.total;
                                const recentAccuracy = perf.recent.reduce((a, b) => a + b, 0) / perf.recent.length;
                                let newWeight = (accuracy * 0.6 + recentAccuracy * 0.3);
                                newWeight = Math.max(0.1, Math.min(2.5, newWeight * 2));
                                this.algorithmWeights[algo.id] = this.algorithmWeights[algo.id] * 0.7 + newWeight * 0.3;
                            }
                        }
                    } catch (e) {}
                });
            }
            console.log('✅ Fine-tuning trên dữ liệu thực hoàn tất!');

            // LSTM real-time fine-tuning
            if (lstmModel && this.history.length >= 30) {
                console.log('🧠 LSTM fine-tuning on real-time data...');
                const recent = this.history.slice(-100);
                let lstmCorrect = 0, lstmCount = 0;
                for (let i = 20; i < recent.length - 1; i++) {
                    const pastTx = recent.slice(0, i + 1).map(h => h.tx);
                    const actualTx = recent[i + 1].tx;
                    try {
                        const seq = lstmModel.encodeSequence(pastTx.slice(-20));
                        const targetIdx = actualTx === 'T' ? 0 : 1;
                        if (lstmModel.trainSteps < 50000) lstmModel.trainStep(seq, targetIdx);
                        const pred = lstmModel.forwardSequence(seq);
                        const predIdx = pred.probs[0] > pred.probs[1] ? 0 : 1;
                        if (predIdx === targetIdx) lstmCorrect++;
                        lstmCount++;
                    } catch (e) {}
                }
                if (lstmCount > 0) {
                    lstmModel.lastAccuracy = lstmCorrect / lstmCount;
                    console.log(`   LSTM real-time acc: ${(lstmModel.lastAccuracy * 100).toFixed(1)}%`);
                }
            }
        }
    }

    getPattern() {
        if (this.history.length < 50) return { recent: 'đang thu thập...', long: 'đang thu thập...' };
        const tx = this.history.map(h => h.tx);
        const recent = tx.slice(-20).join('').toLowerCase();
        const long = tx.slice(-50).join('').toLowerCase();
        return {
            recent, long,
            discovered: this.discoverDominantPattern(tx.slice(-30))
        };
    }

    discoverDominantPattern(txArray) {
        const str = txArray.join('').toLowerCase();
        let dominantPattern = null, maxOccurrences = 0;
        Object.entries(PATTERN_DATABASE).forEach(([name, patterns]) => {
            patterns.forEach(pattern => {
                let count = 0;
                for (let i = 0; i <= str.length - pattern.length; i++) {
                    if (str.substr(i, pattern.length) === pattern) count++;
                }
                if (count > maxOccurrences) {
                    maxOccurrences = count;
                    dominantPattern = name;
                }
            });
        });
        return dominantPattern || 'không xác định';
    }

    getStats() {
        const stats = {};
        ALGORITHMS.forEach(algo => {
            const perf = this.algorithmPerformance[algo.id];
            if (perf.total > 0) {
                stats[algo.id] = {
                    name: perf.name,
                    accuracy: (perf.correct / perf.total * 100).toFixed(1) + '%',
                    weight: this.algorithmWeights[algo.id].toFixed(2),
                    predictions: perf.total,
                    streak: perf.streak
                };
            }
        });
        return stats;
    }
}

// --- Khởi tạo AI ---
const ai = new AdvancedDeepLearningAI();
// ============================================================
// PHẦN 5/5: FASTIFY SERVER + WEBSOCKET + SUNWIN CONNECTION
// ============================================================

const app = fastify({ logger: false });
await app.register(cors, { origin: "*" });

// --- HELPER: Broadcast tới tất cả WS clients ---
function broadcastToClients(data) {
    const payload = JSON.stringify(data);
    wsClients.forEach(client => {
        if (client.readyState === 1) { // OPEN
            try { client.send(payload); } catch (e) {}
        }
    });
}

// --- HELPER: Tạo payload dự đoán ---
function buildPredictionPayload() {
    const valid = rikResults.filter((r) => r.dice?.length === 3);
    const lastResult = valid.length ? valid[0] : null;
    const currentPrediction = ai.predict();
    const pattern = ai.getPattern();

    if (!lastResult) {
        return {
            success: false,
            status: "đang chờ dữ liệu phiên đầu tiên...",
            phien_truoc: null, xuc_xac: null, tong: null, ket_qua: null,
            phien_hien_tai: null, du_doan: null, do_tin_cay: "0%",
            pattern_gan_nhat: pattern.recent, pattern_dai: pattern.long,
            pattern_chu_dao: pattern.discovered,
            ai_version: "11.0 - LSTM-Enhanced Ultra AI",
            algorithms_active: 0, total_algorithms: ALGORITHMS.length,
            timestamp: Date.now()
        };
    }

    let lstmInfo = null;
    if (lstmModel) {
        const txHistory = ai.history.map(h => h.tx);
        const lstmPred = lstmModel.predict(txHistory);
        if (lstmPred) {
            lstmInfo = {
                du_doan: lstmPred.prediction === 'T' ? 'tài' : 'xỉu',
                do_tin_cay: `${(lstmPred.confidence * 100).toFixed(1)}%`,
                p_tai: `${(lstmPred.probs[0] * 100).toFixed(1)}%`,
                p_xiu: `${(lstmPred.probs[1] * 100).toFixed(1)}%`
            };
        }
    }

    return {
        success: true,
        status: "online",
        phien_truoc: lastResult.session,
        xuc_xac: lastResult.dice,
        tong: lastResult.total,
        ket_qua: lastResult.result.toLowerCase(),
        phien_hien_tai: lastResult.session + 1,
        du_doan: currentPrediction.prediction,
        do_tin_cay: `${(currentPrediction.confidence * 100).toFixed(1)}%`,
        pattern_gan_nhat: pattern.recent,
        pattern_dai: pattern.long,
        pattern_chu_dao: pattern.discovered,
        ai_version: "11.0 - LSTM-Enhanced Ultra AI",
        algorithms_active: currentPrediction.algorithms,
        total_algorithms: ALGORITHMS.length,
        lstm_prediction: lstmInfo,
        timestamp: Date.now()
    };
}

// ============================================================
// REST API ENDPOINTS
// ============================================================

// GET /api/sunwin/tx
app.get("/api/sunwin/tx", async (request, reply) => {
    try {
        reply.header('Cache-Control', 'no-cache, no-store, must-revalidate');
        reply.header('X-AI-Version', '11.0');
        return buildPredictionPayload();
    } catch (error) {
        console.error('Lỗi API /api/sunwin/tx:', error);
        return reply.status(500).send({
            success: false, error: "Hệ thống đang xử lý lỗi.", message: error.message
        });
    }
});

// GET /api/sunwin/tx/only
app.get("/api/sunwin/tx/only", async (request, reply) => {
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        const lastResult = valid.length ? valid[0] : null;
        const currentPrediction = ai.predict();
        if (!lastResult) {
            return { success: false, du_doan: null, phien: null, do_tin_cay: "0%" };
        }
        return {
            success: true,
            phien: lastResult.session + 1,
            phien_truoc: lastResult.session,
            ket_qua_truoc: lastResult.result.toLowerCase(),
            du_doan: currentPrediction.prediction,
            do_tin_cay: `${(currentPrediction.confidence * 100).toFixed(1)}%`,
            confidence_raw: currentPrediction.confidence,
            algorithms: currentPrediction.algorithms
        };
    } catch (error) {
        return reply.status(500).send({ success: false, error: error.message });
    }
});

// GET /api/sunwin/tx/history
app.get("/api/sunwin/tx/history", async (request, reply) => {
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        if (!valid.length) {
            return { success: false, message: "chưa có dữ liệu.", history: [] };
        }
        const history = valid.slice(0, 30).map((i) => ({
            phien: i.session,
            xuc_xac: i.dice,
            tong: i.total,
            ket_qua: i.result.toLowerCase(),
            tx: i.total >= 11 ? 'T' : 'X'
        }));
        return { success: true, total: history.length, history };
    } catch (e) {
        return reply.status(500).send({ success: false, error: e.message });
    }
});

// GET /api/taixiu/sunwin (endpoint cũ - giữ nguyên)
app.get("/api/taixiu/sunwin", async (request, reply) => {
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        const lastResult = valid.length ? valid[0] : null;
        const currentPrediction = ai.predict();
        const pattern = ai.getPattern();
        if (!lastResult) {
            return {
                id: "GiaThinhzZz Sunwin AI",
                status: "đang chờ dữ liệu phiên đầu tiên...",
                phien_truoc: null, tong: null, ket_qua: "đang chờ...",
                pattern_gan_nhat: pattern.recent, pattern_dai: pattern.long,
                phien_hien_tai: null, du_doan: "đang tính...", do_tin_cay_ai: "50%"
            };
        }
        return {
            id: "GiaThinhzZz Sunwin AI",
            phien_truoc: lastResult.session,
            xuc_xac: lastResult.dice,
            tong: lastResult.total,
            ket_qua: lastResult.result.toLowerCase(),
            pattern_gan_nhat: pattern.recent,
            pattern_dai: pattern.long,
            pattern_chu_dao: pattern.discovered,
            phien_hien_tai: lastResult.session + 1,
            du_doan: currentPrediction.prediction,
            do_tin_cay_ai: `${(currentPrediction.confidence * 100).toFixed(1)}%`,
        };
    } catch (error) {
        return { id: "GiaThinhzZz Sunwin AI", error: "Lỗi hệ thống." };
    }
});

// GET /api/taixiu/history
app.get("/api/taixiu/history", async () => {
    try {
        const valid = rikResults.filter((r) => r.dice?.length === 3);
        if (!valid.length) return { message: "chưa có dữ liệu." };
        return valid.slice(0, 30).map((i) => ({
            session: i.session, dice: i.dice, total: i.total,
            result: i.result.toLowerCase(), tx: i.total >= 11 ? 'T' : 'X'
        }));
    } catch (e) {
        return { message: "lỗi hệ thống" };
    }
});

// GET /api/taixiu/ai-stats
app.get("/api/taixiu/ai-stats", async () => {
    try {
        const stats = ai.getStats();
        const prediction = ai.predict();
        const pattern = ai.getPattern();
        return {
            status: "online",
            ai_version: "11.0 - LSTM-Enhanced Ultra AI",
            dataset_info: {
                total_samples: dataset.totalSamples,
                pattern_length_range: `${dataset.minLen}-${dataset.maxLen}`,
                loaded: dataset.totalSamples > 0
            },
            lstm_info: lstmModel ? {
                enabled: true,
                hidden_size: lstmModel.hiddenSize,
                train_steps: lstmModel.trainSteps,
                last_loss: lstmModel.trainLoss.toFixed(4),
                train_accuracy: `${(lstmModel.lastAccuracy * 100).toFixed(1)}%`
            } : { enabled: false },
            current_prediction: prediction.prediction,
            confidence: `${(prediction.confidence * 100).toFixed(1)}%`,
            algorithms_active: prediction.algorithms,
            total_algorithms: ALGORITHMS.length,
            pattern_dominant: pattern.discovered,
            algorithm_stats: stats,
            ws_clients: wsClients.size
        };
    } catch (e) {
        return { error: "Lỗi hệ thống" };
    }
});

// GET /api/taixiu/lstm-debug
app.get("/api/taixiu/lstm-debug", async () => {
    if (!lstmModel) return { error: "LSTM chưa được khởi tạo" };
    const tx = ai.history.map(h => h.tx);
    if (tx.length < 5) return { error: "Chưa đủ history" };
    const results = {};
    for (const len of [10, 15, 20, 25, 30]) {
        if (tx.length >= len) {
            const seq = lstmModel.encodeSequence(tx.slice(-len));
            const out = lstmModel.forwardSequence(seq);
            results[`len_${len}`] = {
                prediction: out.probs[0] > out.probs[1] ? 'T' : 'X',
                pT: out.probs[0].toFixed(4),
                pX: out.probs[1].toFixed(4),
                confidence: Math.max(out.probs[0], out.probs[1]).toFixed(4)
            };
        }
    }
    return {
        train_steps: lstmModel.trainSteps,
        train_accuracy: lstmModel.lastAccuracy,
        predictions_by_length: results,
        hidden_size: lstmModel.hiddenSize,
        learning_rate: lstmModel.learningRate
    };
});

// GET /
app.get("/", async () => {
    return {
        status: "online",
        name: "GiaThinhzZz Sunwin AI",
        version: "11.0 - LSTM-Enhanced Ultra AI",
        description: "Hệ thống AI dự đoán với 14 thuật toán + LSTM sequence model",
        algorithms_count: ALGORITHMS.length,
        pattern_database: Object.keys(PATTERN_DATABASE).length + " mẫu cầu",
        dataset_samples: dataset.totalSamples,
        ws_clients: wsClients.size,
        endpoints: {
            rest: [
                "GET /api/sunwin/tx",
                "GET /api/sunwin/tx/only",
                "GET /api/sunwin/tx/history",
                "GET /api/taixiu/sunwin",
                "GET /api/taixiu/history",
                "GET /api/taixiu/ai-stats",
                "GET /api/taixiu/lstm-debug"
            ],
            websocket: [
                "WS /ws/sunwin/tx"
            ]
        },
        features: [
            "14 AI Algorithms (incl. 2 LSTM)",
            "10,000 pattern dataset",
            "N-Gram Markov Chain",
            "Pure-JS LSTM with BPTT",
            "Real-time WebSocket push"
        ]
    };
});

// ============================================================
// WEBSOCKET SERVER (cho clients subscribe)
// ============================================================

// Sử dụng plugin @fastify/websocket (cần cài: npm i @fastify/websocket)
import websocketPlugin from "@fastify/websocket";
await app.register(websocketPlugin);

app.register(async function (fastify) {
    fastify.get('/ws/sunwin/tx', { websocket: true }, (socket, req) => {
        console.log(`🔌 WS client connected | Total: ${wsClients.size + 1}`);
        wsClients.add(socket);

        // Gửi payload hiện tại ngay khi kết nối
        try {
            socket.send(JSON.stringify({
                type: 'welcome',
                ...buildPredictionPayload()
            }));
        } catch (e) {}

        // Xử lý message từ client (ping, subscribe...)
        socket.on('message', (msg) => {
            try {
                const data = JSON.parse(msg.toString());
                if (data.type === 'ping') {
                    socket.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                } else if (data.type === 'get') {
                    socket.send(JSON.stringify({
                        type: 'prediction',
                        ...buildPredictionPayload()
                    }));
                }
            } catch (e) {}
        });

        socket.on('close', () => {
            wsClients.delete(socket);
            console.log(`🔌 WS client disconnected | Total: ${wsClients.size}`);
        });

        socket.on('error', (err) => {
            console.error('WS client error:', err.message);
            wsClients.delete(socket);
        });
    });
});

// ============================================================
// SERVER START
// ============================================================
const start = async () => {
    try {
        await app.listen({ port: PORT, host: "0.0.0.0" });
        console.log(`\n==============================================`);
        console.log(`🚀 GiaThinhzZz Sunwin AI ULTRA Server v11.0`);
        console.log(`==============================================`);
        console.log(`   Port: ${PORT}`);
        console.log(`   Thuật toán: ${ALGORITHMS.length} AI Algorithms`);
        console.log(`   Pattern Database: ${Object.keys(PATTERN_DATABASE).length} mẫu`);
        console.log(`   Dataset: ${dataset.totalSamples} mẫu cầu`);
        console.log(`   LSTM: ${lstmModel ? 'ENABLED' : 'DISABLED'}`);
        console.log(`   WS Endpoint: ws://localhost:${PORT}/ws/sunwin/tx`);
        console.log(`==============================================`);
    } catch (err) {
        console.error('❌ Lỗi khởi động server:', err);
        process.exit(1);
    }
};

// ============================================================
// SUNWIN WEBSOCKET CONNECTION (nhận dữ liệu từ game)
// ============================================================
function decodeBinaryMessage(data) {
    try {
        const message = new TextDecoder().decode(data);
        if (message.startsWith("[") || message.startsWith("{")) return JSON.parse(message);
        return null;
    } catch { return null; }
}

function sendRikCmd1005() {
    if (rikWS?.readyState === WebSocket.OPEN) {
        try {
            rikWS.send(JSON.stringify([6, "MiniGame", "taixiuPlugin", { cmd: 1005 }]));
        } catch (e) {
            console.error("Lỗi gửi lệnh 1005:", e.message);
        }
    }
}

function connectRikWebSocket() {
    console.log("\n🔌 Đang kết nối Sunwin WebSocket...");
    if (rikWS && (rikWS.readyState === WebSocket.OPEN || rikWS.readyState === WebSocket.CONNECTING)) {
        rikWS.close();
    }
    clearInterval(rikIntervalCmd);

    try {
        rikWS = new WebSocket(`${WS_URL}${TOKEN}`);
    } catch (e) {
        console.error("Lỗi tạo WebSocket:", e.message);
        setTimeout(connectRikWebSocket, 5000);
        return;
    }

    rikWS.on("open", () => {
        console.log("✅ Sunwin WebSocket connected - Đang xác thực...");
        const authPayload = [1, "MiniGame", "SC_giathinh2133", "thinh211", {
            info: JSON.stringify({
                ipAddress: "2402:800:62cd:b4d1:8c64:a3c9:12bf:c19a",
                wsToken: TOKEN,
                userId: "cdbaf598-e4ef-47f8-b4a6-a4881098db86",
                username: "SC_hellokietne212",
                timestamp: Date.now(),
            }),
            signature: "473ABDDDA6BDD74D8F0B6036223B0E3A002A518203A9BB9F95AD763E3BF969EC2CBBA61ED1A3A9E217B52A4055658D7BEA38F89B806285974C7F3F62A9400066709B4746585887D00C9796552671894F826E69EFD234F6778A5DDC24830CEF68D51217EF047644E0B0EB1CB26942EB34AEF114AEC36A6DF833BB10F7D122EA5E",
            pid: 5, subi: true,
        }];
        try { rikWS.send(JSON.stringify(authPayload)); } catch (e) {}
        rikIntervalCmd = setInterval(sendRikCmd1005, 5000);
    });

    rikWS.on("message", (data) => {
        try {
            const json = typeof data === "string" ? JSON.parse(data) : decodeBinaryMessage(data);
            if (!json) return;

            if (json.session && Array.isArray(json.dice)) {
                const record = {
                    session: json.session, dice: json.dice,
                    total: json.total, result: json.result,
                };
                const parsed = ai.addResult(record);
                if (!rikCurrentSession || record.session > rikCurrentSession) {
                    rikCurrentSession = record.session;
                    rikResults.unshift(record);
                    if (rikResults.length > 100) rikResults.pop();
                }
                const prediction = ai.predict();
                console.log(`\n==============================================`);
                console.log(`📥 PHIÊN ${parsed.session}: ${parsed.result} (${parsed.total})`);
                console.log(`🔮 DỰ ĐOÁN ${parsed.session + 1}: **${prediction.prediction.toUpperCase()}**`);
                console.log(`🎯 CONFIDENCE: ${(prediction.confidence * 100).toFixed(1)}%`);
                console.log(`🤖 ALGORITHMS: ${prediction.algorithms}/${ALGORITHMS.length}`);

                // === BROADCAST TỚI TẤT CẢ WS CLIENTS ===
                broadcastToClients({
                    type: 'new_result',
                    ...buildPredictionPayload()
                });
            }
            else if (Array.isArray(json) && json[1]?.htr) {
                const newHistory = json[1].htr.map((i) => ({
                    session: i.sid,
                    dice: [i.d1, i.d2, i.d3],
                    total: i.d1 + i.d2 + i.d3,
                    result: i.d1 + i.d2 + i.d3 >= 11 ? "Tài" : "Xỉu",
                })).sort((a, b) => a.session - b.session);

                ai.loadHistory(newHistory);
                rikResults = newHistory.slice(-50).sort((a, b) => b.session - a.session);

                const prediction = ai.predict();
                console.log(`\n==============================================`);
                console.log(`📊 Đã tải ${newHistory.length} kết quả lịch sử`);
                console.log(`🤖 ULTRA PATTERN AI ĐÃ SẴN SÀNG`);
                console.log(`🎯 Confidence: ${(prediction.confidence * 100).toFixed(1)}%`);

                // Broadcast khi có history mới
                broadcastToClients({
                    type: 'history_loaded',
                    history_count: newHistory.length,
                    ...buildPredictionPayload()
                });
            }
        } catch (e) {
            console.error("❌ Parse message error:", e.message);
        }
    });

    rikWS.on("close", () => {
        console.log("🔌 Sunwin WebSocket disconnected. Reconnecting in 3s...");
        clearInterval(rikIntervalCmd);
        setTimeout(connectRikWebSocket, 3000);
    });

    rikWS.on("error", (err) => {
        console.error("🔌 Sunwin WebSocket error:", err.message);
        rikWS.close();
    });
}

// --- Khởi động ---
start().then(() => {
    connectRikWebSocket();
}).catch(err => {
    console.error('Failed to start application:', err);
});