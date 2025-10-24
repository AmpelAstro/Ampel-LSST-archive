# generated with avro-to-python-types

from base64 import b64encode
from typing import Annotated, TypedDict

from pydantic import PlainSerializer

# NB: ser_json_bytes="base64" uses a URL-safe alphabet, whereas b64encode
# uses +/ to represent 62 and 63. Use a serialization function to emit
# strings that can be properly decoded with b64decode. See:
# https://github.com/pydantic/pydantic/issues/7000
Base64Bytes = Annotated[
    bytes,
    PlainSerializer(lambda v: b64encode(v).decode(), return_type=str, when_used="json"),
]


class LsstV9_0DiaSource(TypedDict):
    diaSourceId: int
    visit: int
    detector: int
    diaObjectId: int | None
    ssObjectId: int | None
    parentDiaSourceId: int | None
    midpointMjdTai: float
    ra: float
    raErr: float | None
    dec: float
    decErr: float | None
    ra_dec_Cov: float | None
    x: float
    xErr: float | None
    y: float
    yErr: float | None
    centroid_flag: bool | None
    apFlux: float | None
    apFluxErr: float | None
    apFlux_flag: bool | None
    apFlux_flag_apertureTruncated: bool | None
    isNegative: bool | None
    snr: float | None
    psfFlux: float | None
    psfFluxErr: float | None
    psfLnL: float | None
    psfChi2: float | None
    psfNdata: int | None
    psfFlux_flag: bool | None
    psfFlux_flag_edge: bool | None
    psfFlux_flag_noGoodPixels: bool | None
    trailFlux: float | None
    trailFluxErr: float | None
    trailRa: float | None
    trailRaErr: float | None
    trailDec: float | None
    trailDecErr: float | None
    trailLength: float | None
    trailLengthErr: float | None
    trailAngle: float | None
    trailAngleErr: float | None
    trailChi2: float | None
    trailNdata: int | None
    trail_flag_edge: bool | None
    dipoleMeanFlux: float | None
    dipoleMeanFluxErr: float | None
    dipoleFluxDiff: float | None
    dipoleFluxDiffErr: float | None
    dipoleLength: float | None
    dipoleAngle: float | None
    dipoleChi2: float | None
    dipoleNdata: int | None
    scienceFlux: float | None
    scienceFluxErr: float | None
    forced_PsfFlux_flag: bool | None
    forced_PsfFlux_flag_edge: bool | None
    forced_PsfFlux_flag_noGoodPixels: bool | None
    templateFlux: float | None
    templateFluxErr: float | None
    ixx: float | None
    iyy: float | None
    ixy: float | None
    ixxPSF: float | None
    iyyPSF: float | None
    ixyPSF: float | None
    shape_flag: bool | None
    shape_flag_no_pixels: bool | None
    shape_flag_not_contained: bool | None
    shape_flag_parent_source: bool | None
    extendedness: float | None
    reliability: float | None
    band: str | None
    isDipole: bool | None
    dipoleFitAttempted: bool | None
    timeProcessedMjdTai: float
    timeWithdrawnMjdTai: float | None
    bboxSize: int | None
    pixelFlags: bool | None
    pixelFlags_bad: bool | None
    pixelFlags_cr: bool | None
    pixelFlags_crCenter: bool | None
    pixelFlags_edge: bool | None
    pixelFlags_nodata: bool | None
    pixelFlags_nodataCenter: bool | None
    pixelFlags_interpolated: bool | None
    pixelFlags_interpolatedCenter: bool | None
    pixelFlags_offimage: bool | None
    pixelFlags_saturated: bool | None
    pixelFlags_saturatedCenter: bool | None
    pixelFlags_suspect: bool | None
    pixelFlags_suspectCenter: bool | None
    pixelFlags_streak: bool | None
    pixelFlags_streakCenter: bool | None
    pixelFlags_injected: bool | None
    pixelFlags_injectedCenter: bool | None
    pixelFlags_injected_template: bool | None
    pixelFlags_injected_templateCenter: bool | None
    glint_trail: bool | None


class LsstV9_0DiaForcedSource(TypedDict):
    diaForcedSourceId: int
    diaObjectId: int
    ra: float
    dec: float
    visit: int
    detector: int
    psfFlux: float | None
    psfFluxErr: float | None
    midpointMjdTai: float
    scienceFlux: float | None
    scienceFluxErr: float | None
    band: str | None
    timeProcessedMjdTai: float
    timeWithdrawnMjdTai: float | None


class LsstV9_0DiaObject(TypedDict):
    diaObjectId: int
    validityStartMjdTai: float
    ra: float
    raErr: float | None
    dec: float
    decErr: float | None
    ra_dec_Cov: float | None
    u_psfFluxMean: float | None
    u_psfFluxMeanErr: float | None
    u_psfFluxSigma: float | None
    u_psfFluxNdata: int | None
    u_fpFluxMean: float | None
    u_fpFluxMeanErr: float | None
    g_psfFluxMean: float | None
    g_psfFluxMeanErr: float | None
    g_psfFluxSigma: float | None
    g_psfFluxNdata: int | None
    g_fpFluxMean: float | None
    g_fpFluxMeanErr: float | None
    r_psfFluxMean: float | None
    r_psfFluxMeanErr: float | None
    r_psfFluxSigma: float | None
    r_psfFluxNdata: int | None
    r_fpFluxMean: float | None
    r_fpFluxMeanErr: float | None
    i_psfFluxMean: float | None
    i_psfFluxMeanErr: float | None
    i_psfFluxSigma: float | None
    i_psfFluxNdata: int | None
    i_fpFluxMean: float | None
    i_fpFluxMeanErr: float | None
    z_psfFluxMean: float | None
    z_psfFluxMeanErr: float | None
    z_psfFluxSigma: float | None
    z_psfFluxNdata: int | None
    z_fpFluxMean: float | None
    z_fpFluxMeanErr: float | None
    y_psfFluxMean: float | None
    y_psfFluxMeanErr: float | None
    y_psfFluxSigma: float | None
    y_psfFluxNdata: int | None
    y_fpFluxMean: float | None
    y_fpFluxMeanErr: float | None
    u_scienceFluxMean: float | None
    u_scienceFluxMeanErr: float | None
    g_scienceFluxMean: float | None
    g_scienceFluxMeanErr: float | None
    r_scienceFluxMean: float | None
    r_scienceFluxMeanErr: float | None
    i_scienceFluxMean: float | None
    i_scienceFluxMeanErr: float | None
    z_scienceFluxMean: float | None
    z_scienceFluxMeanErr: float | None
    y_scienceFluxMean: float | None
    y_scienceFluxMeanErr: float | None
    u_psfFluxMin: float | None
    u_psfFluxMax: float | None
    u_psfFluxMaxSlope: float | None
    u_psfFluxErrMean: float | None
    g_psfFluxMin: float | None
    g_psfFluxMax: float | None
    g_psfFluxMaxSlope: float | None
    g_psfFluxErrMean: float | None
    r_psfFluxMin: float | None
    r_psfFluxMax: float | None
    r_psfFluxMaxSlope: float | None
    r_psfFluxErrMean: float | None
    i_psfFluxMin: float | None
    i_psfFluxMax: float | None
    i_psfFluxMaxSlope: float | None
    i_psfFluxErrMean: float | None
    z_psfFluxMin: float | None
    z_psfFluxMax: float | None
    z_psfFluxMaxSlope: float | None
    z_psfFluxErrMean: float | None
    y_psfFluxMin: float | None
    y_psfFluxMax: float | None
    y_psfFluxMaxSlope: float | None
    y_psfFluxErrMean: float | None
    firstDiaSourceMjdTai: float | None
    lastDiaSourceMjdTai: float | None
    nDiaSources: int


class LsstV9_0SsSource(TypedDict):
    ssObjectId: int | None
    diaSourceId: int | None
    eclipticLambda: float | None
    eclipticBeta: float | None
    galacticL: float | None
    galacticB: float | None
    phaseAngle: float | None
    heliocentricDist: float | None
    topocentricDist: float | None
    predictedVMagnitude: float | None
    residualRa: float | None
    residualDec: float | None
    heliocentricX: float | None
    heliocentricY: float | None
    heliocentricZ: float | None
    heliocentricVX: float | None
    heliocentricVY: float | None
    heliocentricVZ: float | None
    topocentricX: float | None
    topocentricY: float | None
    topocentricZ: float | None
    topocentricVX: float | None
    topocentricVY: float | None
    topocentricVZ: float | None


class LsstV9_0MPCORB(TypedDict):
    mpcDesignation: str | None
    ssObjectId: int | None
    mpcH: float | None
    epoch: float | None
    M: float | None
    peri: float | None
    node: float | None
    incl: float | None
    e: float | None
    a: float | None
    q: float | None
    t_p: float | None


class LsstV9_0Alert(TypedDict):
    diaSourceId: int
    observation_reason: str | None
    target_name: str | None
    diaSource: LsstV9_0DiaSource
    prvDiaSources: list[LsstV9_0DiaSource] | None
    prvDiaForcedSources: list[LsstV9_0DiaForcedSource] | None
    diaObject: LsstV9_0DiaObject | None
    ssSource: LsstV9_0SsSource | None
    MPCORB: LsstV9_0MPCORB | None
    cutoutDifference: Base64Bytes | None
    cutoutScience: Base64Bytes | None
    cutoutTemplate: Base64Bytes | None
